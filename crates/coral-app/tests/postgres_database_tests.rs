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

/// Pins the source artifact tables against the schema a real Postgres-configured
/// boot leaves behind.
///
/// `SourceManifestsRepo` and `MaterializationsRepo` are covered in-crate against
/// both backends over a pool the harness migrates itself. What is asserted here
/// is what only Postgres can answer: that `source_document_raw` is a real
/// `bytea` and returns the bytes it was given rather than a text round trip,
/// that the two optional artifact columns are the only nullable ones, and that
/// removing a source — or the workspace above it — takes its artifacts with it.
#[tokio::test]
#[ignore = "set CORAL_TEST_POSTGRES_URL to run the source artifact write contract"]
async fn source_artifacts_hold_their_write_contract_after_a_postgres_boot() {
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

    assert_manifest_round_trips_and_replaces_in_place(&pool).await;
    assert_materialization_keeps_the_raw_document_byte_for_byte(&pool).await;
    assert_optional_artifacts_are_the_only_nullable_columns(&pool).await;
    assert_artifacts_cascade_from_the_source_and_the_workspace(&pool).await;

    server.shutdown().await.expect("shutdown server");
}

/// A manifest reads back as what was written, a second write replaces it in
/// place, and a manifest for a source this database does not have is refused.
async fn assert_manifest_round_trips_and_replaces_in_place(pool: &PgPool) {
    let (workspace_id, source_name) = install_source_with_children(pool).await;

    upsert_manifest(
        pool,
        &workspace_id,
        &source_name,
        MANIFEST_YAML,
        MANIFEST_HASH,
        10,
    )
    .await
    .expect("store manifest");
    assert_eq!(
        manifest_row(pool, &workspace_id, &source_name).await,
        Some((MANIFEST_YAML.to_owned(), MANIFEST_HASH.to_owned(), 10))
    );

    upsert_manifest(
        pool,
        &workspace_id,
        &source_name,
        REPLACEMENT_MANIFEST_YAML,
        REPLACEMENT_MANIFEST_HASH,
        20,
    )
    .await
    .expect("replace manifest");
    assert_eq!(
        manifest_row(pool, &workspace_id, &source_name).await,
        Some((
            REPLACEMENT_MANIFEST_YAML.to_owned(),
            REPLACEMENT_MANIFEST_HASH.to_owned(),
            20,
        )),
        "a manifest is replaced wholesale, so the write restates its timestamp"
    );
    assert_eq!(
        artifact_counts(pool, &workspace_id).await,
        (1, 0),
        "a source has one manifest, however many times it is written"
    );

    assert!(
        upsert_manifest(
            pool,
            &workspace_id,
            "source_this_database_never_installed",
            MANIFEST_YAML,
            MANIFEST_HASH,
            10,
        )
        .await
        .is_err(),
        "a manifest hangs off a catalog row, so one without a source must be refused"
    );
}

/// The raw source document survives Postgres as the bytes it went in as, out of
/// a column that is genuinely `bytea` rather than text that happened to hold
/// them, and a replacement restates those bytes rather than appending a row.
async fn assert_materialization_keeps_the_raw_document_byte_for_byte(pool: &PgPool) {
    let (workspace_id, source_name) = install_source_with_children(pool).await;

    let stored = MaterializationArtifacts {
        fingerprint_yaml: Some("inputs:\n  - orders.sql\n".to_owned()),
        diagnostics_yaml: Some("warnings: []\n".to_owned()),
        source_document_raw: RAW_DOCUMENT.to_vec(),
    };
    upsert_materialization(pool, &workspace_id, &source_name, &stored, 10)
        .await
        .expect("store materialization");

    let read_back = materialization_row(pool, &workspace_id, &source_name)
        .await
        .expect("a stored materialization reads back");
    assert_eq!(
        read_back.source_document_raw, RAW_DOCUMENT,
        "the raw source document must survive Postgres byte for byte"
    );
    assert_eq!(
        read_back,
        MaterializationRow {
            materialization_version: MATERIALIZATION_VERSION.to_owned(),
            fingerprint_yaml: stored.fingerprint_yaml.clone(),
            diagnostics_yaml: stored.diagnostics_yaml.clone(),
            source_document_raw: RAW_DOCUMENT.to_vec(),
            created_at_unix_nanos: 10,
        }
    );
    assert_eq!(
        artifact_column_type(pool, "source_document_raw").await,
        "bytea",
        "the raw document must be stored as bytes, not as a lossy text column"
    );

    let replacement = MaterializationArtifacts {
        source_document_raw: REPLACEMENT_RAW_DOCUMENT.to_vec(),
        ..stored
    };
    upsert_materialization(pool, &workspace_id, &source_name, &replacement, 20)
        .await
        .expect("replace materialization");
    let replaced = materialization_row(pool, &workspace_id, &source_name)
        .await
        .expect("a replaced materialization reads back");
    assert_eq!(replaced.source_document_raw, REPLACEMENT_RAW_DOCUMENT);
    assert_eq!(replaced.created_at_unix_nanos, 20);
    assert_eq!(
        artifact_counts(pool, &workspace_id).await,
        (0, 1),
        "a source has one materialization, however many times it is written"
    );
}

/// The fingerprint and the diagnostics are the only artifacts the v4 loader
/// treats as optional, so they are the only columns the schema lets go missing —
/// and a write without them stores SQL NULL rather than an empty string.
async fn assert_optional_artifacts_are_the_only_nullable_columns(pool: &PgPool) {
    assert_eq!(
        nullable_columns(pool, "source_manifests").await,
        Vec::<String>::new(),
        "every part of a stored manifest is required"
    );
    assert_eq!(
        nullable_columns(pool, "materializations").await,
        ["diagnostics_yaml", "fingerprint_yaml"]
    );

    let (workspace_id, source_name) = install_source_with_children(pool).await;
    let without_optionals = MaterializationArtifacts {
        fingerprint_yaml: None,
        diagnostics_yaml: None,
        source_document_raw: RAW_DOCUMENT.to_vec(),
    };
    upsert_materialization(pool, &workspace_id, &source_name, &without_optionals, 10)
        .await
        .expect("store materialization without optionals");

    let read_back = materialization_row(pool, &workspace_id, &source_name)
        .await
        .expect("a stored materialization reads back");
    assert_eq!(read_back.fingerprint_yaml, None);
    assert_eq!(read_back.diagnostics_yaml, None);
}

/// Removing a source takes its artifacts with it, and so does removing the
/// workspace above it: the cascade runs the whole chain, not just one link.
async fn assert_artifacts_cascade_from_the_source_and_the_workspace(pool: &PgPool) {
    let (workspace_id, source_name) = install_source_with_children(pool).await;
    write_both_artifacts(pool, &workspace_id, &source_name).await;
    assert_eq!(artifact_counts(pool, &workspace_id).await, (1, 1));

    delete_source(pool, &workspace_id, &source_name).await;
    assert_eq!(
        artifact_counts(pool, &workspace_id).await,
        (0, 0),
        "artifacts must not outlive the source row they describe"
    );

    let (workspace_id, source_name) = install_source_with_children(pool).await;
    write_both_artifacts(pool, &workspace_id, &source_name).await;
    sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(&workspace_id)
        .execute(pool)
        .await
        .expect("delete workspace");
    assert_eq!(
        artifact_counts(pool, &workspace_id).await,
        (0, 0),
        "artifacts must not outlive the workspace that scoped them"
    );
    assert_eq!(source_count(pool, &workspace_id).await, 0);
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

/// Bytes chosen to break anything that round-trips a `bytea` through text: an
/// embedded NUL, a lone `0xFF` that is not valid UTF-8, and a trailing NUL a
/// C-style truncation would eat.
const RAW_DOCUMENT: &[u8] = &[0x00, 0x01, 0xFF, 0xFE, b'y', b'a', b'm', b'l', 0x00];
const REPLACEMENT_RAW_DOCUMENT: &[u8] = &[0xFF, 0x00, b'v', b'2'];

const MANIFEST_YAML: &str = "dsl_version: 4\nname: orders\n";
const REPLACEMENT_MANIFEST_YAML: &str = "dsl_version: 4\nname: shipments\n";

/// The hash column holds opaque text here on purpose: which digest a manifest
/// gets is the repository's business and is pinned in-crate, while what this
/// file asserts is that the column carries whatever it was handed.
const MANIFEST_HASH: &str = "0f4d1c3a";
const REPLACEMENT_MANIFEST_HASH: &str = "9b27ae60";

const MATERIALIZATION_VERSION: &str = "v4";

/// The artifacts one materialization write varies. The rest of the row is the
/// same fixed YAML every time, because what is under test is the columns rather
/// than the artifact contents.
struct MaterializationArtifacts {
    fingerprint_yaml: Option<String>,
    diagnostics_yaml: Option<String>,
    source_document_raw: Vec<u8>,
}

/// One materialization row as Postgres hands it back.
#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct MaterializationRow {
    materialization_version: String,
    fingerprint_yaml: Option<String>,
    diagnostics_yaml: Option<String>,
    source_document_raw: Vec<u8>,
    created_at_unix_nanos: i64,
}

/// Stores one manifest, replacing any manifest already held for the source.
async fn upsert_manifest(
    pool: &PgPool,
    workspace_id: &str,
    source_name: &str,
    manifest_yaml: &str,
    manifest_hash: &str,
    now_unix_nanos: i64,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO source_manifests (
             workspace_id, source_name, manifest_yaml, manifest_hash, created_at_unix_nanos
         )
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (workspace_id, source_name) DO UPDATE SET
             manifest_yaml = EXCLUDED.manifest_yaml,
             manifest_hash = EXCLUDED.manifest_hash,
             created_at_unix_nanos = EXCLUDED.created_at_unix_nanos",
    )
    .bind(workspace_id)
    .bind(source_name)
    .bind(manifest_yaml)
    .bind(manifest_hash)
    .bind(now_unix_nanos)
    .execute(pool)
    .await
    .map(|_| ())
}

async fn manifest_row(
    pool: &PgPool,
    workspace_id: &str,
    source_name: &str,
) -> Option<(String, String, i64)> {
    sqlx::query_as(
        "SELECT manifest_yaml, manifest_hash, created_at_unix_nanos
         FROM source_manifests
         WHERE workspace_id = $1 AND source_name = $2",
    )
    .bind(workspace_id)
    .bind(source_name)
    .fetch_optional(pool)
    .await
    .expect("read the stored manifest")
}

/// Stores one materialization, replacing any row already held for the source.
///
/// The conflict clause restates only the columns these tests vary rather than
/// transcribing the repository's, so the statement carries no copy of the
/// repository's write that could drift out from under it. What is exercised
/// here is the migrated table: its keys, its nullability, and its `bytea`.
async fn upsert_materialization(
    pool: &PgPool,
    workspace_id: &str,
    source_name: &str,
    artifacts: &MaterializationArtifacts,
    now_unix_nanos: i64,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO materializations (
             workspace_id,
             source_name,
             materialization_version,
             fingerprint_yaml,
             projections_yaml,
             diagnostics_yaml,
             source_document_raw,
             source_document_yaml,
             semantic_ir_yaml,
             operation_metadata_yaml,
             created_at_unix_nanos
         )
         VALUES (
             $1, $2, $3, $4, 'projections:\n  - name: orders\n', $5, $6,
             'document:\n  kind: source\n', 'ir:\n  version: 4\n', 'operations: []\n', $7
         )
         ON CONFLICT (workspace_id, source_name) DO UPDATE SET
             fingerprint_yaml = EXCLUDED.fingerprint_yaml,
             diagnostics_yaml = EXCLUDED.diagnostics_yaml,
             source_document_raw = EXCLUDED.source_document_raw,
             created_at_unix_nanos = EXCLUDED.created_at_unix_nanos",
    )
    .bind(workspace_id)
    .bind(source_name)
    .bind(MATERIALIZATION_VERSION)
    .bind(artifacts.fingerprint_yaml.as_deref())
    .bind(artifacts.diagnostics_yaml.as_deref())
    .bind(artifacts.source_document_raw.as_slice())
    .bind(now_unix_nanos)
    .execute(pool)
    .await
    .map(|_| ())
}

async fn materialization_row(
    pool: &PgPool,
    workspace_id: &str,
    source_name: &str,
) -> Option<MaterializationRow> {
    sqlx::query_as(
        "SELECT materialization_version, fingerprint_yaml, diagnostics_yaml,
                source_document_raw, created_at_unix_nanos
         FROM materializations
         WHERE workspace_id = $1 AND source_name = $2",
    )
    .bind(workspace_id)
    .bind(source_name)
    .fetch_optional(pool)
    .await
    .expect("read the stored materialization")
}

/// Writes one manifest and one materialization for a source, so a cascade has
/// both artifact kinds to take with it.
async fn write_both_artifacts(pool: &PgPool, workspace_id: &str, source_name: &str) {
    upsert_manifest(
        pool,
        workspace_id,
        source_name,
        MANIFEST_YAML,
        MANIFEST_HASH,
        10,
    )
    .await
    .expect("store manifest");
    upsert_materialization(
        pool,
        workspace_id,
        source_name,
        &MaterializationArtifacts {
            fingerprint_yaml: None,
            diagnostics_yaml: None,
            source_document_raw: RAW_DOCUMENT.to_vec(),
        },
        10,
    )
    .await
    .expect("store materialization");
}

/// Counts a workspace's `(manifest, materialization)` rows.
async fn artifact_counts(pool: &PgPool, workspace_id: &str) -> (i64, i64) {
    let mut counts = [0_i64; 2];
    for (slot, statement) in counts.iter_mut().zip([
        "SELECT COUNT(*) FROM source_manifests WHERE workspace_id = $1",
        "SELECT COUNT(*) FROM materializations WHERE workspace_id = $1",
    ]) {
        *slot = sqlx::query_scalar(statement)
            .bind(workspace_id)
            .fetch_one(pool)
            .await
            .expect("count artifact rows");
    }
    (counts[0], counts[1])
}

/// Reports the physical Postgres type of one `materializations` column.
async fn artifact_column_type(pool: &PgPool, column: &str) -> String {
    sqlx::query_scalar(
        "SELECT data_type FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = 'materializations'
           AND column_name = $1",
    )
    .bind(column)
    .fetch_one(pool)
    .await
    .expect("read the migrated column type")
}

/// Names the columns of one migrated table that accept SQL NULL.
async fn nullable_columns(pool: &PgPool, table: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = $1 AND is_nullable = 'YES'
         ORDER BY column_name",
    )
    .bind(table)
    .fetch_all(pool)
    .await
    .expect("read the migrated column nullability")
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
