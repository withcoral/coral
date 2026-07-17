use std::time::Duration;

use sea_query::{Expr, ExprTrait, Query};
use tempfile::tempdir;

use super::identity_specs::{
    IdentitySpecDocumentRecord, IdentitySpecId, IdentitySpecKey, IdentitySpecRecord,
};
use crate::bootstrap::{self, AppError};
use crate::encrypted_document::EncryptedEnvelopeDocument;
use crate::state::db::schema::IdentitySpecDocuments;
use crate::state::db::{CoralDb, CoralTx, DbRepos, ResolvedDatabaseConfig};
use crate::workspaces::WorkspaceName;
use coral_spec::{IdentityManifest, parse_identity_manifest_yaml};

#[tokio::test]
async fn identity_spec_persistence_contract_holds_against_sqlite() {
    let temp = tempdir().expect("temp dir");
    let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
        path: temp.path().join("coral.sqlite"),
    })
    .await
    .expect("open sqlite");
    db.migrate().await.expect("migrate sqlite");
    assert_identity_spec_persistence_contract(&db).await;
}

#[tokio::test]
#[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared contract against Postgres"]
async fn identity_spec_persistence_contract_on_postgres() {
    let Some(url) = bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
        .expect("read CORAL_TEST_POSTGRES_URL")
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
        .await
        .expect("open postgres");
    db.migrate().await.expect("migrate postgres");
    assert_identity_spec_persistence_contract(&db).await;
}

#[expect(clippy::too_many_lines, reason = "shared backend contract fixture")]
async fn assert_identity_spec_persistence_contract(db: &CoralDb) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let workspace = parsed_workspace(&format!("identity{suffix}"));
    let alternate = parsed_workspace(&format!("alternate{suffix}"));
    let reserved = parsed_workspace("__global__");
    let shared_name = format!("shared_{suffix}");
    let global = IdentitySpecKey::global(&shared_name).expect("global key");
    let global_zeta = IdentitySpecKey::global(&format!("zeta_{suffix}")).expect("global key");
    let workspace_shared = scoped_key(&workspace, &shared_name);
    let workspace_beta = scoped_key(&workspace, &format!("beta_{suffix}"));
    let alternate_shared = scoped_key(&alternate, &shared_name);
    let reserved_shared = scoped_key(&reserved, &shared_name);

    let mut tx = db.begin().await.expect("begin seed transaction");
    for workspace in [&workspace, &alternate] {
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("ensure workspace");
    }
    for (key, label, now) in [(&global, "global", 10), (&global_zeta, "zeta", 11)] {
        seed_pair(&mut tx, key, label, now).await;
    }
    tx.workspaces()
        .ensure(reserved.as_str(), 1)
        .await
        .expect("ensure reserved-name workspace");
    for (key, label, now) in [
        (&workspace_shared, "workspace", 20),
        (&workspace_beta, "beta", 21),
        (&alternate_shared, "alternate", 30),
        (&reserved_shared, "reserved", 40),
    ] {
        seed_pair(&mut tx, key, label, now).await;
    }
    let global_record = upsert_spec(&mut tx, &global, "replacement", 5)
        .await
        .expect("replace spec with stale clock");
    tx.identity_spec_documents()
        .upsert(&global_record.id, &document("replacement"), 5)
        .await
        .expect("replace document with stale clock");
    tx.commit().await.expect("commit seed transaction");

    assert_pair(db, &global, "replacement", 10, 2).await;
    assert_pair(db, &workspace_shared, "workspace", 20, 1).await;
    assert_pair(db, &reserved_shared, "reserved", 40, 1).await;
    assert_fixture_scope_lists(
        db,
        &global,
        &workspace_shared,
        &suffix,
        [&shared_name, global_zeta.name()],
        [workspace_beta.name(), &shared_name],
    )
    .await;

    let mut tx = db.begin().await.expect("begin delete transaction");
    let alternate_record = tx
        .identity_specs()
        .get(&alternate_shared)
        .await
        .expect("read alternate spec")
        .expect("alternate spec exists");
    assert!(
        tx.identity_spec_documents()
            .delete(&alternate_record.id)
            .await
            .expect("delete document")
    );
    assert!(
        tx.identity_specs()
            .get(&alternate_shared)
            .await
            .expect("read surviving spec")
            .is_some()
    );
    tx.identity_spec_documents()
        .upsert(&alternate_record.id, &document("alternate"), 31)
        .await
        .expect("restore document");
    assert!(
        tx.identity_specs()
            .delete(&workspace_beta)
            .await
            .expect("delete exact spec")
    );
    tx.commit().await.expect("commit exact deletes");
    assert_absent(db, &workspace_beta).await;

    let mut tx = db.begin().await.expect("begin workspace cascade");
    let alternate_id = tx
        .identity_specs()
        .get(&alternate_shared)
        .await
        .expect("read alternate before cascade")
        .expect("alternate exists before cascade")
        .id;
    tx.workspaces()
        .delete(alternate.as_str())
        .await
        .expect("delete workspace");
    tx.commit().await.expect("commit workspace cascade");
    assert_absent(db, &alternate_shared).await;
    let mut session = db;
    assert!(
        session
            .identity_spec_documents()
            .get(&alternate_id)
            .await
            .expect("read cascaded document")
            .is_none()
    );
    assert_pair(db, &global, "replacement", 10, 2).await;

    assert_foreign_keys(db, &suffix).await;
    assert_rollback_invisibility(db, &suffix).await;
    assert_max_version_is_nonmutating(db, &reserved_shared).await;
    assert_concurrent_document_versions(db, &global_zeta).await;

    let mut tx = db.begin().await.expect("begin cleanup transaction");
    for key in [&global, &global_zeta] {
        assert!(
            tx.identity_specs()
                .delete(key)
                .await
                .expect("delete global spec")
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

async fn assert_fixture_scope_lists(
    db: &CoralDb,
    global: &IdentitySpecKey,
    workspace: &IdentitySpecKey,
    suffix: &str,
    mut expected_global: [&str; 2],
    mut expected_workspace: [&str; 2],
) {
    expected_global.sort_unstable();
    expected_workspace.sort_unstable();
    let mut session = db;
    let global_records = session
        .identity_specs()
        .list(global.scope())
        .await
        .expect("list global specs");
    let workspace_records = session
        .identity_specs()
        .list(workspace.scope())
        .await
        .expect("list workspace specs");
    assert_eq!(fixture_names(&global_records, suffix), expected_global);
    assert_eq!(
        fixture_names(&workspace_records, suffix),
        expected_workspace
    );
    assert_eq!(
        session
            .identity_specs()
            .get(workspace)
            .await
            .expect("read exact workspace spec")
            .map(|record| record.version),
        Some("v-workspace".to_string())
    );
}

async fn assert_foreign_keys(db: &CoralDb, suffix: &str) {
    let missing_workspace = parsed_workspace(&format!("missing{suffix}"));
    let missing_spec = scoped_key(&missing_workspace, &format!("missing_{suffix}"));
    let mut tx = db
        .begin()
        .await
        .expect("begin missing workspace transaction");
    assert!(matches!(
        upsert_spec(&mut tx, &missing_spec, "missing", 50).await,
        Err(AppError::Database(_))
    ));
    tx.rollback().await.expect("rollback failed transaction");
    assert_absent(db, &missing_spec).await;

    let orphan_id = IdentitySpecId::new();
    let mut tx = db.begin().await.expect("begin orphan document transaction");
    assert!(matches!(
        tx.identity_spec_documents()
            .upsert(&orphan_id, &document("orphan"), 51)
            .await,
        Err(AppError::Database(_))
    ));
    tx.rollback().await.expect("rollback failed transaction");
    let mut session = db;
    assert!(
        session
            .identity_spec_documents()
            .get(&orphan_id)
            .await
            .expect("read missing orphan document")
            .is_none()
    );
}

async fn assert_rollback_invisibility(db: &CoralDb, suffix: &str) {
    let key = IdentitySpecKey::global(&format!("rollback_{suffix}")).expect("rollback key");
    let mut tx = db.begin().await.expect("begin rollback transaction");
    seed_pair(&mut tx, &key, "rollback", 60).await;
    tx.rollback().await.expect("rollback pair");
    assert_absent(db, &key).await;
}

async fn assert_max_version_is_nonmutating(db: &CoralDb, key: &IdentitySpecKey) {
    let mut tx = db.begin().await.expect("begin max-version transaction");
    let identity_spec_id = tx
        .identity_specs()
        .get(key)
        .await
        .expect("read max-version spec")
        .expect("max-version spec exists")
        .id;
    tx.execute(
        Query::update()
            .table(IdentitySpecDocuments::Table)
            .value(IdentitySpecDocuments::DocumentVersion, i64::MAX)
            .and_where(document_id_where(&identity_spec_id))
            .to_owned(),
    )
    .await
    .expect("set max document version");
    let before = tx
        .identity_spec_documents()
        .get(&identity_spec_id)
        .await
        .expect("read max-version document")
        .expect("document exists");
    let error = tx
        .identity_spec_documents()
        .upsert(&identity_spec_id, &document("overflow"), 70)
        .await
        .expect_err("max version must not wrap");
    assert!(matches!(error, AppError::FailedPrecondition(_)));
    assert_eq!(
        tx.identity_spec_documents()
            .get(&identity_spec_id)
            .await
            .expect("reread max-version document")
            .expect("document remains"),
        before
    );
    tx.rollback().await.expect("rollback max-version change");
}

async fn assert_concurrent_document_versions(db: &CoralDb, key: &IdentitySpecKey) {
    let mut session = db;
    let identity_spec_id = session
        .identity_specs()
        .get(key)
        .await
        .expect("read concurrent-update spec")
        .expect("concurrent-update spec exists")
        .id;
    let barrier = tokio::sync::Barrier::new(2);
    let (left, right) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            replace_document(db, &identity_spec_id, "left", 81, &barrier),
            replace_document(db, &identity_spec_id, "right", 82, &barrier)
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
    identity_spec_id: &IdentitySpecId,
    label: &str,
    now: i64,
    barrier: &tokio::sync::Barrier,
) -> i64 {
    let mut tx = db.begin().await.expect("begin document update");
    barrier.wait().await;
    let version = tx
        .identity_spec_documents()
        .upsert(identity_spec_id, &document(label), now)
        .await
        .expect("replace document")
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
    let spec = upsert_spec(tx, key, label, now).await.expect("upsert spec");
    let document = tx
        .identity_spec_documents()
        .upsert(&spec.id, &document(label), now)
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
        .get(key)
        .await
        .expect("read spec")
        .expect("spec exists");
    let persisted_document = session
        .identity_spec_documents()
        .get(&spec.id)
        .await
        .expect("read document")
        .expect("document exists");
    assert_eq!(spec, expected_spec(spec.id.clone(), key, label, now));
    assert_eq!(
        persisted_document,
        IdentitySpecDocumentRecord {
            identity_spec_id: spec.id,
            document_version,
            envelope: document(label),
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
            .get(key)
            .await
            .expect("read spec")
            .is_none()
    );
}

fn expected_spec(
    id: IdentitySpecId,
    key: &IdentitySpecKey,
    label: &str,
    now: i64,
) -> IdentitySpecRecord {
    let (manifest, manifest_yaml) = spec(key, label);
    IdentitySpecRecord {
        id,
        key: key.clone(),
        version: manifest.version,
        description: manifest.description,
        issuer: manifest.issuer,
        identity_type: "fixed_token".to_string(),
        manifest_yaml,
        created_at_unix_nanos: now,
        updated_at_unix_nanos: now,
    }
}

pub(super) async fn upsert_spec(
    tx: &mut CoralTx<'_>,
    key: &IdentitySpecKey,
    label: &str,
    now: i64,
) -> Result<IdentitySpecRecord, AppError> {
    let (manifest, manifest_yaml) = spec(key, label);
    tx.identity_specs()
        .upsert(key, &manifest, &manifest_yaml, now)
        .await
}

fn spec(key: &IdentitySpecKey, label: &str) -> (IdentityManifest, String) {
    let manifest_yaml = format!(
        "kind: identity\nspec_version: 1\nname: {}\nversion: v-{label}\ndescription: description-{label}\nissuer: issuer_{label}\ntype: fixed_token\naudience:\n  host: api.example.com\n",
        key.name()
    );
    let manifest =
        parse_identity_manifest_yaml(&manifest_yaml).expect("valid identity spec manifest");
    (manifest, manifest_yaml)
}

pub(super) fn document(label: &str) -> EncryptedEnvelopeDocument {
    EncryptedEnvelopeDocument::new(
        format!("cipher-{label}").into_bytes(),
        format!("nonce-{label}").into_bytes(),
        format!("wrapped-{label}").into_bytes(),
        format!("wrapped-nonce-{label}").into_bytes(),
        format!("key-{label}"),
        format!("algorithm-{label}"),
        99,
    )
    .expect("valid opaque document")
}

fn document_id_where(identity_spec_id: &IdentitySpecId) -> sea_query::SimpleExpr {
    Expr::col(IdentitySpecDocuments::IdentitySpecId).eq(identity_spec_id.as_str())
}

fn names(records: &[IdentitySpecRecord]) -> Vec<&str> {
    records.iter().map(|record| record.key.name()).collect()
}

fn fixture_names<'a>(records: &'a [IdentitySpecRecord], suffix: &str) -> Vec<&'a str> {
    names(records)
        .into_iter()
        .filter(|name| name.ends_with(suffix))
        .collect()
}

fn parsed_workspace(name: &str) -> WorkspaceName {
    WorkspaceName::parse(name).expect("valid workspace")
}

fn scoped_key(workspace: &WorkspaceName, name: &str) -> IdentitySpecKey {
    IdentitySpecKey::workspace(workspace.clone(), name).expect("workspace key")
}
