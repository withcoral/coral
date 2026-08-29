//! Pins gRPC server startup behavior for configured Postgres storage.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use coral_client::local::ServerBuilder;
use coral_engine::{
    CoralQuery, QueryRuntimeConfig, QuerySource, RuntimeSourceComponent, RuntimeSourcePackage,
};
use coral_spec::{
    DatabaseConnectionSpec, DatabaseSourceManifest, ParsedTemplate, PostgresConnectionSpec,
    SourceManifestCommon,
};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tempfile::TempDir;

#[tokio::test]
#[ignore = "set CORAL_TEST_POSTGRES_URL to run configured Postgres startup coverage"]
async fn server_lifecycle_can_start_with_postgres_database_config() {
    let Some(database_url) = postgres_test_url() else {
        return;
    };
    let temp = TempDir::new().expect("temp dir");
    let config_dir = postgres_config_dir(&temp);

    // `make postgres-tests` points every Postgres test at one database, where
    // the legacy default name is now an ordinary one somebody may legitimately
    // have created, so what is asserted is what this startup adds rather than
    // what the database already held.
    let legacy_default_before = count_legacy_default_workspaces(&database_url)
        .await
        .unwrap_or(0);

    let server = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .start()
        .await
        .expect("start server with Postgres config");
    let legacy_default_after = count_legacy_default_workspaces(&database_url)
        .await
        .expect("startup should migrate the workspaces table");
    assert_eq!(
        legacy_default_after, legacy_default_before,
        "startup must not invent a 'default' workspace"
    );

    server.shutdown().await.expect("shutdown server");
}

/// Pins the source catalog's write contract against the schema a real
/// Postgres-configured boot leaves behind.
///
/// `SourcesRepo`'s own behavior is covered in-crate against both backends over
/// a pool that harness migrates itself. What is asserted here is the other
/// half: that the catalog a booted server actually provisions holds the writes
/// that repository makes — the same install, update, remove, and tombstone
/// sequence, issued straight at the tables so a drift between the migrated
/// schema and the statements the repository builds fails a statement rather
/// than passing a self-provisioned harness.
#[tokio::test]
#[ignore = "set CORAL_TEST_POSTGRES_URL to run the source catalog write contract"]
async fn source_catalog_holds_its_write_contract_after_a_postgres_boot() {
    let Some(database_url) = postgres_test_url() else {
        return;
    };
    let temp = TempDir::new().expect("temp dir");
    let server = ServerBuilder::new()
        .with_config_dir(postgres_config_dir(&temp))
        .start()
        .await
        .expect("start server with Postgres config");
    let pool = PgPoolOptions::new()
        .connect(&database_url)
        .await
        .expect("open Postgres database");

    assert_installed_source_round_trips(&pool).await;
    assert_update_keeps_the_install_time_and_rekeys_the_child_sets(&pool).await;
    assert_removal_cascades_children_and_leaves_the_tombstone(&pool).await;
    assert_workspace_deletion_cascades_tombstones(&pool).await;

    server.shutdown().await.expect("shutdown server");
}

/// An install writes one parent row and its two child sets, and reads back as
/// what was written.
async fn assert_installed_source_round_trips(pool: &PgPool) {
    let (workspace_id, source_name) = install_source_with_children(pool).await;

    let installed: (Option<String>, String, Option<String>, String, i64, i64) = sqlx::query_as(
        "SELECT version, origin_kind, credential_storage, credential_revision,
                created_at_unix_nanos, updated_at_unix_nanos
         FROM sources
         WHERE workspace_id = $1 AND name = $2",
    )
    .bind(&workspace_id)
    .bind(&source_name)
    .fetch_one(pool)
    .await
    .expect("read the installed source row");
    assert_eq!(
        installed,
        (
            Some("1.2.3".to_owned()),
            "imported".to_owned(),
            Some("file".to_owned()),
            INSTALL_REVISION.to_owned(),
            10,
            10,
        )
    );
    assert_eq!(
        variables(pool, &workspace_id, &source_name).await,
        [("region".to_owned(), "us-east-1".to_owned())]
    );
    assert_eq!(
        secret_keys(pool, &workspace_id, &source_name).await,
        ["api_token"]
    );
}

/// An update restates everything but when the source was installed, and the
/// child sets are keyed — rewriting one is a conflict, not a second row, which
/// is why the repository replaces those sets wholesale instead of merging.
async fn assert_update_keeps_the_install_time_and_rekeys_the_child_sets(pool: &PgPool) {
    let (workspace_id, source_name) = install_source_with_children(pool).await;

    upsert_source(pool, &workspace_id, &source_name, None, 20)
        .await
        .expect("update source");
    let updated: (Option<String>, i64, i64) = sqlx::query_as(
        "SELECT version, created_at_unix_nanos, updated_at_unix_nanos
         FROM sources
         WHERE workspace_id = $1 AND name = $2",
    )
    .bind(&workspace_id)
    .bind(&source_name)
    .fetch_one(pool)
    .await
    .expect("read the updated source row");
    assert_eq!(
        updated,
        (None, 10, 20),
        "an update must not restate when the source was installed"
    );

    assert!(
        insert_variable(pool, &workspace_id, &source_name, "region", "eu-west-1")
            .await
            .is_err(),
        "a variable key is an identity, so rewriting it in place must conflict"
    );
    assert!(
        insert_secret_key(pool, &workspace_id, &source_name, "api_token")
            .await
            .is_err(),
        "a secret key names a set member, so naming it twice must conflict"
    );

    delete_child_rows(pool, &workspace_id, &source_name).await;
    insert_variable(pool, &workspace_id, &source_name, "region", "eu-west-1")
        .await
        .expect("replace variable");
    insert_secret_key(pool, &workspace_id, &source_name, "rotated_token")
        .await
        .expect("replace secret key");
    assert_eq!(
        variables(pool, &workspace_id, &source_name).await,
        [("region".to_owned(), "eu-west-1".to_owned())],
        "the replaced set must carry the new value"
    );
    assert_eq!(
        secret_keys(pool, &workspace_id, &source_name).await,
        ["rotated_token"],
        "the replaced set must not keep the key it replaced"
    );
}

/// Removing a source takes its child rows with it and leaves the deletion
/// record standing: the tombstone carries no foreign key to the row it records.
async fn assert_removal_cascades_children_and_leaves_the_tombstone(pool: &PgPool) {
    let (workspace_id, source_name) = install_source_with_children(pool).await;

    upsert_tombstone(pool, &workspace_id, &source_name, 30).await;
    delete_source(pool, &workspace_id, &source_name).await;

    assert_eq!(source_count(pool, &workspace_id).await, 0);
    assert!(
        variables(pool, &workspace_id, &source_name)
            .await
            .is_empty()
    );
    assert!(
        secret_keys(pool, &workspace_id, &source_name)
            .await
            .is_empty()
    );
    assert_eq!(
        tombstone_deleted_at(pool, &workspace_id, &source_name).await,
        Some(30),
        "the deletion record must outlive the row it records"
    );

    // Deleting again records the later removal over the earlier one.
    upsert_tombstone(pool, &workspace_id, &source_name, 31).await;
    assert_eq!(
        tombstone_deleted_at(pool, &workspace_id, &source_name).await,
        Some(31)
    );

    // Re-adding the source is what revokes the record.
    delete_tombstone(pool, &workspace_id, &source_name).await;
    upsert_source(pool, &workspace_id, &source_name, Some("1.2.3"), 40)
        .await
        .expect("re-add source");
    assert_eq!(
        tombstone_deleted_at(pool, &workspace_id, &source_name).await,
        None
    );
    assert_eq!(source_count(pool, &workspace_id).await, 1);
}

/// A deleted workspace takes its deletion records with it: a tombstone outlives
/// its source but not the workspace that scoped it, so a re-created workspace
/// does not inherit an old host's removals.
async fn assert_workspace_deletion_cascades_tombstones(pool: &PgPool) {
    let (workspace_id, source_name) = fresh_catalog_ids();
    insert_workspace(pool, &workspace_id).await;
    // Recorded without a source row on purpose: a removal this database never
    // saw installed is still written, and must still be scoped to a workspace.
    upsert_tombstone(pool, &workspace_id, &source_name, 30).await;

    sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(&workspace_id)
        .execute(pool)
        .await
        .expect("delete workspace");

    assert_eq!(
        tombstone_deleted_at(pool, &workspace_id, &source_name).await,
        None
    );
}

#[tokio::test]
#[ignore = "set CORAL_TEST_POSTGRES_URL to run Postgres source inventory coverage"]
async fn postgres_source_inventory_reads_information_schema_domain_columns_as_utf8() {
    let Some(database_url) = postgres_test_url() else {
        return;
    };
    let pool = PgPoolOptions::new()
        .connect(&database_url)
        .await
        .expect("open Postgres database");
    sqlx::query("CREATE SCHEMA IF NOT EXISTS coral_inventory")
        .execute(&pool)
        .await
        .expect("create inventory fixture schema");
    sqlx::query("DROP TABLE IF EXISTS coral_inventory.column_types")
        .execute(&pool)
        .await
        .expect("reset inventory fixture table");
    sqlx::query(
        "CREATE TABLE coral_inventory.column_types (
            id BIGINT NOT NULL,
            display_name CHARACTER VARYING(64),
            note TEXT
        )",
    )
    .execute(&pool)
    .await
    .expect("create inventory fixture table");

    let source = postgres_source(&database_url);
    let tables = CoralQuery::list_tables(
        &[source],
        QueryRuntimeConfig::default(),
        Some("postgres_inventory"),
        Some("coral_inventory"),
        Some("column_types"),
    )
    .await
    .expect("read Postgres column inventory through coral.columns");

    assert_eq!(tables.len(), 1);
    let columns = &tables.first().expect("inventory fixture table").columns;
    assert_eq!(columns.len(), 3);
    let id = columns.first().expect("id column metadata");
    assert_eq!(id.name, "id");
    assert_eq!(id.data_type, "bigint");
    assert!(!id.nullable);
    assert_eq!(id.ordinal_position, 0);
    let display_name = columns.get(1).expect("display_name column metadata");
    assert_eq!(display_name.name, "display_name");
    assert_eq!(display_name.data_type, "character varying");
    assert!(display_name.nullable);
    assert_eq!(display_name.ordinal_position, 1);

    sqlx::query("DROP SCHEMA coral_inventory CASCADE")
        .execute(&pool)
        .await
        .expect("remove inventory fixture schema");
}

fn postgres_source(database_url: &str) -> QuerySource {
    let url = url::Url::parse(database_url).expect("parse Postgres test URL");
    let host = url.host_str().expect("Postgres test URL host");
    let port = url.port_or_known_default().expect("Postgres test URL port");
    let database = url.path().trim_start_matches('/');
    let sslmode = url
        .query_pairs()
        .find_map(|(key, value)| (key == "sslmode").then(|| value.into_owned()))
        .unwrap_or_else(|| {
            if matches!(host, "127.0.0.1" | "localhost" | "::1") {
                "disable".to_string()
            } else {
                "verify-full".to_string()
            }
        });
    let template = |value: &str| ParsedTemplate::parse(value).expect("literal template");
    let manifest = DatabaseSourceManifest {
        common: SourceManifestCommon {
            dsl_version: 4,
            name: "postgres_inventory".to_string(),
            version: String::new(),
            description: "Postgres inventory integration fixture".to_string(),
            test_queries: Vec::new(),
        },
        connection: DatabaseConnectionSpec::Postgres(PostgresConnectionSpec {
            host: template(host),
            port: template(&port.to_string()),
            database: template(database),
            user: template(url.username()),
            password: template(url.password().unwrap_or_default()),
            sslmode: Some(template(&sslmode)),
        }),
        declared_inputs: Vec::new(),
    };
    QuerySource::from_runtime_components(
        RuntimeSourcePackage {
            source_name: "postgres_inventory".to_string(),
            authored_version: None,
            description: String::new(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            components: vec![RuntimeSourceComponent::Database(manifest)],
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("build Postgres inventory source")
}

/// Counts the workspace rows holding the name a fresh install used to be given.
///
/// Reports `None` while the schema is unmigrated, which is how a first run
/// against an empty database tells "no table yet" from "no such row". Asking
/// about that one name rather than about a total is deliberate: `make
/// postgres-tests` points every Postgres test at a single database, so the row
/// count belongs to no test in particular.
async fn count_legacy_default_workspaces(database_url: &str) -> Option<i64> {
    let pool = PgPoolOptions::new()
        .connect(database_url)
        .await
        .expect("open Postgres database");
    let table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('public.workspaces') IS NOT NULL")
            .fetch_one(&pool)
            .await
            .expect("inspect migrated Postgres schema");
    if !table_exists {
        return None;
    }

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workspaces WHERE id = $1")
        .bind("default")
        .fetch_one(&pool)
        .await
        .expect("count the workspace rows holding the legacy default name");
    Some(count)
}

/// Writes a config directory that points a server at the test database.
fn postgres_config_dir(temp: &TempDir) -> PathBuf {
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        "[database]\nbackend = \"postgres\"\nurl_env = \"CORAL_TEST_POSTGRES_URL\"\n",
    )
    .expect("write config");
    config_dir
}

/// The credential revision an install writes; the column holds it as text.
const INSTALL_REVISION: &str = "6dcf7b1e-4a10-4c8c-9f6f-2f1a0d2b7c31";

/// A workspace and source name no other test shares, because `make
/// postgres-tests` points every Postgres test at one database.
fn fresh_catalog_ids() -> (String, String) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    (format!("workspace_{suffix}"), format!("source_{suffix}"))
}

/// Installs one source with one variable and one secret key under a workspace
/// of its own, and reports the ids it generated.
async fn install_source_with_children(pool: &PgPool) -> (String, String) {
    let (workspace_id, source_name) = fresh_catalog_ids();
    insert_workspace(pool, &workspace_id).await;
    upsert_source(pool, &workspace_id, &source_name, Some("1.2.3"), 10)
        .await
        .expect("install source");
    insert_variable(pool, &workspace_id, &source_name, "region", "us-east-1")
        .await
        .expect("install variable");
    insert_secret_key(pool, &workspace_id, &source_name, "api_token")
        .await
        .expect("install secret key");
    (workspace_id, source_name)
}

async fn insert_workspace(pool: &PgPool, workspace_id: &str) {
    sqlx::query("INSERT INTO workspaces (id, created_at_unix_nanos) VALUES ($1, 1)")
        .bind(workspace_id)
        .execute(pool)
        .await
        .expect("insert workspace");
}

/// Installs or updates one source through the repository's conflict clause,
/// which deliberately leaves `created_at_unix_nanos` alone on an update.
async fn upsert_source(
    pool: &PgPool,
    workspace_id: &str,
    source_name: &str,
    version: Option<&str>,
    now_unix_nanos: i64,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO sources (
             workspace_id,
             name,
             version,
             origin_kind,
             credential_storage,
             credential_revision,
             created_at_unix_nanos,
             updated_at_unix_nanos
         )
         VALUES ($1, $2, $3, 'imported', 'file', $4, $5, $5)
         ON CONFLICT (workspace_id, name) DO UPDATE SET
             version = EXCLUDED.version,
             origin_kind = EXCLUDED.origin_kind,
             credential_storage = EXCLUDED.credential_storage,
             credential_revision = EXCLUDED.credential_revision,
             updated_at_unix_nanos = EXCLUDED.updated_at_unix_nanos",
    )
    .bind(workspace_id)
    .bind(source_name)
    .bind(version)
    .bind(INSTALL_REVISION)
    .bind(now_unix_nanos)
    .execute(pool)
    .await
    .map(|_| ())
}

async fn insert_variable(
    pool: &PgPool,
    workspace_id: &str,
    source_name: &str,
    key: &str,
    value: &str,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO source_variables (workspace_id, source_name, key, value)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(workspace_id)
    .bind(source_name)
    .bind(key)
    .bind(value)
    .execute(pool)
    .await
    .map(|_| ())
}

async fn insert_secret_key(
    pool: &PgPool,
    workspace_id: &str,
    source_name: &str,
    key: &str,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO source_secret_keys (workspace_id, source_name, key)
         VALUES ($1, $2, $3)",
    )
    .bind(workspace_id)
    .bind(source_name)
    .bind(key)
    .execute(pool)
    .await
    .map(|_| ())
}

async fn upsert_tombstone(
    pool: &PgPool,
    workspace_id: &str,
    source_name: &str,
    deleted_at_unix_nanos: i64,
) {
    sqlx::query(
        "INSERT INTO source_tombstones (workspace_id, source_name, deleted_at_unix_nanos)
         VALUES ($1, $2, $3)
         ON CONFLICT (workspace_id, source_name) DO UPDATE SET
             deleted_at_unix_nanos = EXCLUDED.deleted_at_unix_nanos",
    )
    .bind(workspace_id)
    .bind(source_name)
    .bind(deleted_at_unix_nanos)
    .execute(pool)
    .await
    .expect("record the deletion");
}

async fn delete_tombstone(pool: &PgPool, workspace_id: &str, source_name: &str) {
    sqlx::query("DELETE FROM source_tombstones WHERE workspace_id = $1 AND source_name = $2")
        .bind(workspace_id)
        .bind(source_name)
        .execute(pool)
        .await
        .expect("revoke the deletion record");
}

async fn delete_source(pool: &PgPool, workspace_id: &str, source_name: &str) {
    sqlx::query("DELETE FROM sources WHERE workspace_id = $1 AND name = $2")
        .bind(workspace_id)
        .bind(source_name)
        .execute(pool)
        .await
        .expect("delete source");
}

async fn delete_child_rows(pool: &PgPool, workspace_id: &str, source_name: &str) {
    for statement in [
        "DELETE FROM source_variables WHERE workspace_id = $1 AND source_name = $2",
        "DELETE FROM source_secret_keys WHERE workspace_id = $1 AND source_name = $2",
    ] {
        sqlx::query(statement)
            .bind(workspace_id)
            .bind(source_name)
            .execute(pool)
            .await
            .expect("delete child rows");
    }
}

async fn source_count(pool: &PgPool, workspace_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM sources WHERE workspace_id = $1")
        .bind(workspace_id)
        .fetch_one(pool)
        .await
        .expect("count sources")
}

async fn variables(pool: &PgPool, workspace_id: &str, source_name: &str) -> Vec<(String, String)> {
    sqlx::query_as(
        "SELECT key, value FROM source_variables
         WHERE workspace_id = $1 AND source_name = $2
         ORDER BY key",
    )
    .bind(workspace_id)
    .bind(source_name)
    .fetch_all(pool)
    .await
    .expect("read source variables")
}

async fn secret_keys(pool: &PgPool, workspace_id: &str, source_name: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT key FROM source_secret_keys
         WHERE workspace_id = $1 AND source_name = $2
         ORDER BY key",
    )
    .bind(workspace_id)
    .bind(source_name)
    .fetch_all(pool)
    .await
    .expect("read source secret keys")
}

async fn tombstone_deleted_at(pool: &PgPool, workspace_id: &str, source_name: &str) -> Option<i64> {
    sqlx::query_scalar(
        "SELECT deleted_at_unix_nanos FROM source_tombstones
         WHERE workspace_id = $1 AND source_name = $2",
    )
    .bind(workspace_id)
    .bind(source_name)
    .fetch_optional(pool)
    .await
    .expect("read the deletion record")
}

#[expect(
    clippy::disallowed_methods,
    reason = "The ignored Postgres integration test is explicitly gated by this CI/test-only variable."
)]
fn postgres_test_url() -> Option<String> {
    std::env::var("CORAL_TEST_POSTGRES_URL")
        .ok()
        .filter(|value| !value.is_empty())
}
