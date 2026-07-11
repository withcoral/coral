use std::sync::Arc;

use sqlx::migrate::Migrator;
use tempfile::tempdir;

use super::{CoralDbBackend, MIGRATOR};
use crate::bootstrap;
use crate::state::db::repositories::identity_specs_contract_tests::{
    assert_identity_spec_write_contract, document,
};
use crate::state::db::repositories::identity_specs_negative_contract_tests::assert_identity_spec_negative_contract;
use crate::state::db::{
    CoralDb, DbRepos, IdentitySpecKey, IdentitySpecWrite, ResolvedDatabaseConfig,
};

#[tokio::test]
async fn production_migrate_fills_sparse_version_gaps_on_sqlite() {
    let temp = tempdir().expect("temp dir");
    let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
        path: temp.path().join("coral.sqlite"),
    })
    .await
    .expect("open sqlite");
    assert_migration_order_contract(&db).await;
}

#[tokio::test]
#[ignore = "set CORAL_TEST_POSTGRES_URL to run the isolated Postgres database contracts"]
async fn postgres_identity_database_contracts() {
    let base_url = bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
        .expect("read CORAL_TEST_POSTGRES_URL")
        .filter(|value| !value.is_empty())
        .expect("CORAL_TEST_POSTGRES_URL must be set for the Postgres contract");
    let schema = format!("coral_b1d_{}", uuid::Uuid::new_v4().simple());
    let admin = sqlx::PgPool::connect(&base_url)
        .await
        .expect("connect Postgres admin pool");
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("create isolated schema");
    let mut isolated_url = url::Url::parse(&base_url).expect("parse Postgres URL");
    isolated_url
        .query_pairs_mut()
        .append_pair("options[search_path]", &schema);
    let db = Arc::new(
        CoralDb::open(ResolvedDatabaseConfig::Postgres {
            url: isolated_url.to_string(),
        })
        .await
        .expect("open isolated Postgres database"),
    );
    let CoralDbBackend::Postgres(backend) = &db.backend else {
        panic!("expected Postgres backend");
    };
    let current_schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(&backend.pool)
        .await
        .expect("read current schema");
    assert_eq!(current_schema, schema);
    assert_migration_order_contract(&db).await;
    assert_repeatable_read_snapshot_contract(&db).await;
    crate::state::db::repositories::identity_specs::tests::assert_identity_spec_read_contract(&db)
        .await;
    assert_identity_spec_write_contract(&db).await;
    assert_identity_spec_negative_contract(&db).await;
    crate::identity_specs::manager::tests::assert_identity_spec_mutation_contract(&db).await;

    backend.pool.close().await;
    drop(db);
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .expect("drop isolated schema");
    admin.close().await;
}

async fn assert_repeatable_read_snapshot_contract(db: &CoralDb) {
    let name = format!("snapshot_{}", uuid::Uuid::new_v4().simple());
    let key = IdentitySpecKey::global(&name).expect("snapshot key");
    let write = |version: &str| {
        IdentitySpecWrite::new(
            version,
            "snapshot",
            "issuer",
            "fixed_token",
            format!(
                "kind: identity\nspec_version: 1\nname: {name}\nversion: {version}\ndescription: snapshot\nissuer: issuer\ntype: fixed_token\n"
            ),
        )
        .expect("snapshot write")
    };
    let mut seed = db.begin().await.expect("begin snapshot seed");
    seed.identity_specs()
        .upsert(&key, &write("before"), 1)
        .await
        .expect("seed snapshot record");
    seed.identity_spec_documents()
        .upsert(&key, &document("before"), 1)
        .await
        .expect("seed snapshot document");
    seed.commit().await.expect("commit snapshot seed");
    let mut snapshot = db.begin_read_snapshot().await.expect("begin read snapshot");
    let before = snapshot
        .identity_specs()
        .load_optional(&key)
        .await
        .expect("read original snapshot record")
        .expect("original snapshot record");
    assert_eq!(before.version, "before");
    let mut update = db.begin().await.expect("begin concurrent update");
    update
        .identity_specs()
        .upsert(&key, &write("after"), 2)
        .await
        .expect("update snapshot record");
    update
        .identity_spec_documents()
        .upsert(&key, &document("after"), 2)
        .await
        .expect("update snapshot document");
    update.commit().await.expect("commit concurrent update");
    let during = snapshot
        .identity_spec_documents()
        .load_optional(&key)
        .await
        .expect("read snapshot document")
        .expect("snapshot document remains visible");
    assert_eq!(during.ciphertext, b"cipher-before");
    snapshot.commit().await.expect("commit read snapshot");
    let mut fresh = db;
    let current_spec = fresh
        .identity_specs()
        .load_optional(&key)
        .await
        .expect("read current record")
        .expect("current snapshot record");
    let current_document = fresh
        .identity_spec_documents()
        .load_optional(&key)
        .await
        .expect("read current document")
        .expect("current snapshot document");
    assert_eq!(current_spec.version, "after");
    assert_eq!(current_document.ciphertext, b"cipher-after");
}

async fn assert_migration_order_contract(db: &CoralDb) {
    let sparse = Migrator::with_migrations(
        MIGRATOR
            .migrations
            .iter()
            .filter(|migration| matches!(migration.version, 1 | 10))
            .cloned()
            .collect(),
    );
    assert_eq!(migration_versions(&sparse), vec![1, 10]);
    run_migrator(db, &sparse).await;
    assert_eq!(migration_ledger(db).await, vec![1, 10]);
    let mut session = db;
    let identity_specs = session
        .identity_specs()
        .list_global()
        .await
        .expect("use sparsely migrated identity_specs");
    assert!(identity_specs.is_empty());

    db.migrate().await.expect("run production migrations");
    let expected = migration_versions(&MIGRATOR);
    assert!(expected.iter().any(|version| 1 < *version && *version < 10));
    assert_eq!(migration_ledger(db).await, expected);

    let marker = format!("b1d-order-{}", uuid::Uuid::new_v4().simple());
    let mut tx = db.begin().await.expect("begin state-migration tx");
    tx.state_migrations()
        .mark_completed(&marker, 42)
        .await
        .expect("use app_state_migrations");
    tx.commit().await.expect("commit state migration marker");
    assert!(
        session
            .state_migrations()
            .has_completed(&marker)
            .await
            .expect("load state migration marker")
    );

    db.migrate().await.expect("repeat production migrations");
    assert_eq!(migration_ledger(db).await, expected);
    assert!(
        session
            .state_migrations()
            .has_completed(&marker)
            .await
            .expect("marker survives idempotent migrate")
    );
}

async fn run_migrator(db: &CoralDb, migrator: &Migrator) {
    match &db.backend {
        CoralDbBackend::Sqlite(backend) => migrator
            .run(&backend.pool)
            .await
            .expect("run sparse SQLite migrations"),
        CoralDbBackend::Postgres(backend) => migrator
            .run(&backend.pool)
            .await
            .expect("run sparse Postgres migrations"),
    }
}

async fn migration_ledger(db: &CoralDb) -> Vec<i64> {
    let sql = "SELECT version FROM _sqlx_migrations ORDER BY version";
    match &db.backend {
        CoralDbBackend::Sqlite(backend) => sqlx::query_scalar(sql)
            .fetch_all(&backend.pool)
            .await
            .expect("load SQLite migration ledger"),
        CoralDbBackend::Postgres(backend) => sqlx::query_scalar(sql)
            .fetch_all(&backend.pool)
            .await
            .expect("load Postgres migration ledger"),
    }
}

fn migration_versions(migrator: &Migrator) -> Vec<i64> {
    migrator
        .migrations
        .iter()
        .map(|migration| migration.version)
        .collect()
}
