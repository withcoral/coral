//! Pins behavior for configured Postgres storage.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

use std::collections::BTreeMap;
use std::fs;

use coral_api::v1::{
    CreateWorkspaceRequest, DeleteWorkspaceRequest, ListWorkspacesRequest, Workspace,
};
use coral_app::RunningServer;
use coral_client::{AppClient, local::ServerBuilder};
use coral_engine::{
    CoralQuery, QueryRuntimeConfig, QuerySource, RuntimeSourceComponent, RuntimeSourcePackage,
};
use coral_spec::{
    DatabaseConnectionSpec, DatabaseSourceManifest, ParsedTemplate, PostgresConnectionSpec,
    SourceManifestCommon,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use tempfile::TempDir;
use tonic::Request;

#[tokio::test]
async fn server_lifecycle_can_start_with_postgres_database_config() {
    let Some(database_url) = postgres_test_url() else {
        return;
    };
    let server = start_server_with_postgres_config(&database_url).await;
    let pool = open_postgres_pool(&database_url).await;

    assert_postgres_db_is_migrated(&pool).await;
    assert_workspace_service_round_trips_against_postgres(&server.app).await;
    assert_source_catalog_schema_shape(&pool).await;
    assert_source_catalog_rows_round_trip(&pool).await;

    Box::pin(server.shutdown()).await;
}

struct PostgresServer {
    app: AppClient,
    server: RunningServer,
    _temp: TempDir,
}

impl PostgresServer {
    async fn shutdown(self) {
        self.server.shutdown().await.expect("shutdown server");
    }
}

async fn start_server_with_postgres_config(database_url: &str) -> PostgresServer {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        "[database]\nbackend = \"postgres\"\nurl_env = \"CORAL_TEST_POSTGRES_URL\"\n",
    )
    .expect("write config");

    // `make postgres-tests` points every Postgres test at one database, where
    // the legacy default name is now an ordinary one somebody may legitimately
    // have created, so what is asserted is what this startup adds rather than
    // what the database already held.
    let legacy_default_before = count_legacy_default_workspaces(database_url)
        .await
        .unwrap_or(0);

    let server = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .start()
        .await
        .expect("start server with Postgres config");
    let legacy_default_after = count_legacy_default_workspaces(database_url)
        .await
        .expect("startup should migrate the workspaces table");
    assert_eq!(
        legacy_default_after, legacy_default_before,
        "startup must not invent a 'default' workspace"
    );
    let app = AppClient::connect(server.endpoint_uri())
        .await
        .expect("connect Postgres-backed server");

    PostgresServer {
        app,
        server,
        _temp: temp,
    }
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

async fn assert_postgres_db_is_migrated(pool: &PgPool) {
    let table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('public.workspaces') IS NOT NULL")
            .fetch_one(pool)
            .await
            .expect("inspect migrated Postgres schema");
    assert!(table_exists, "workspaces table should be migrated");
}

async fn assert_workspace_service_round_trips_against_postgres(app: &AppClient) {
    let workspace_name = format!("postgres_work_{}", uuid::Uuid::new_v4().simple());
    let workspace = Workspace {
        name: workspace_name.clone(),
    };

    let mut workspace_client = app.workspace_client();
    let created = workspace_client
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace.clone()),
        }))
        .await
        .expect("create workspace")
        .into_inner()
        .workspace
        .expect("created workspace");
    assert_eq!(created.name, workspace_name);

    let listed_after_create = workspace_client
        .list_workspaces(Request::new(ListWorkspacesRequest {}))
        .await
        .expect("list workspaces after create")
        .into_inner()
        .memberships;
    assert!(
        listed_after_create.iter().any(|membership| membership
            .workspace
            .as_ref()
            .is_some_and(|workspace| workspace.name == workspace_name)),
        "created workspace should be listed from Postgres state"
    );

    let duplicate_error = workspace_client
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace.clone()),
        }))
        .await
        .expect_err("duplicate workspace should fail");
    assert_eq!(duplicate_error.code(), tonic::Code::AlreadyExists);

    let deleted = workspace_client
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            workspace: Some(workspace),
        }))
        .await
        .expect("delete workspace")
        .into_inner()
        .workspace
        .expect("deleted workspace");
    assert_eq!(deleted.name, workspace_name);

    let listed_after_delete = workspace_client
        .list_workspaces(Request::new(ListWorkspacesRequest {}))
        .await
        .expect("list workspaces after delete")
        .into_inner()
        .memberships;
    assert!(
        !listed_after_delete.iter().any(|membership| membership
            .workspace
            .as_ref()
            .is_some_and(|workspace| workspace.name == workspace_name)),
        "deleted workspace should not be listed from Postgres state"
    );
}

async fn assert_source_catalog_schema_shape(pool: &PgPool) {
    let secret_key_columns = sqlx::query(
        "SELECT column_name
         FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = 'source_secret_keys'
         ORDER BY ordinal_position",
    )
    .fetch_all(pool)
    .await
    .expect("inspect source_secret_keys columns")
    .into_iter()
    .map(|row| row.get::<String, _>("column_name"))
    .collect::<Vec<_>>();

    assert_eq!(
        secret_key_columns,
        ["workspace_id", "source_name", "key"],
        "source secret keys are a keyed set; declaration order is not persisted"
    );
}

async fn assert_source_catalog_rows_round_trip(pool: &PgPool) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let workspace_id = format!("workspace_{suffix}");
    let source_name = format!("source_{suffix}");

    insert_source_catalog_rows(pool, &workspace_id, &source_name)
        .await
        .expect("insert source catalog rows");
    assert_eq!(
        source_variable_count(pool, &workspace_id, &source_name).await,
        1
    );
    assert_eq!(
        source_secret_key_count(pool, &workspace_id, &source_name).await,
        1
    );

    assert!(
        insert_source_row(pool, &workspace_id, &source_name)
            .await
            .is_err(),
        "duplicate source identity should fail"
    );
    assert!(
        insert_source_variable_row(pool, &workspace_id, &source_name, "REGION", "eu-west-1")
            .await
            .is_err(),
        "duplicate source variable key should fail"
    );
    assert!(
        insert_source_secret_key_row(pool, &workspace_id, &source_name, "OTHER_TOKEN")
            .await
            .is_ok(),
        "distinct source secret keys should be allowed"
    );
    assert!(
        insert_source_secret_key_row(pool, &workspace_id, &source_name, "API_TOKEN")
            .await
            .is_err(),
        "duplicate source secret key should fail"
    );

    let alternate_workspace_id = format!("alternate_workspace_{suffix}");
    insert_source_catalog_rows(pool, &alternate_workspace_id, &source_name)
        .await
        .expect("insert same source name in another workspace");
    assert_eq!(source_count(pool, &alternate_workspace_id).await, 1);
    delete_workspace(pool, &alternate_workspace_id).await;

    delete_source(pool, &workspace_id, &source_name).await;
    assert_eq!(
        source_variable_count(pool, &workspace_id, &source_name).await,
        0
    );
    assert_eq!(
        source_secret_key_count(pool, &workspace_id, &source_name).await,
        0
    );

    insert_source_catalog_rows(pool, &workspace_id, &source_name)
        .await
        .expect("reinsert source catalog rows");
    delete_workspace(pool, &workspace_id).await;
    assert_eq!(source_count(pool, &workspace_id).await, 0);
    assert_eq!(
        source_variable_count(pool, &workspace_id, &source_name).await,
        0
    );
    assert_eq!(
        source_secret_key_count(pool, &workspace_id, &source_name).await,
        0
    );
}

async fn insert_source_catalog_rows(
    pool: &PgPool,
    workspace_id: &str,
    source_name: &str,
) -> sqlx::Result<()> {
    insert_workspace_row(pool, workspace_id).await?;
    insert_source_row(pool, workspace_id, source_name).await?;
    insert_source_variable_row(pool, workspace_id, source_name, "REGION", "us-east-1").await?;
    insert_source_secret_key_row(pool, workspace_id, source_name, "API_TOKEN").await
}

async fn insert_workspace_row(pool: &PgPool, workspace_id: &str) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO workspaces (id, created_at_unix_nanos)
         VALUES ($1, 1)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(workspace_id)
    .execute(pool)
    .await
    .map(|_| ())
}

async fn insert_source_row(
    pool: &PgPool,
    workspace_id: &str,
    source_name: &str,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO sources (
             workspace_id,
             name,
             version,
             origin_kind,
             credential_storage,
             created_at_unix_nanos,
             updated_at_unix_nanos
         )
         VALUES ($1, $2, '1.0.0', 'imported', 'file', 2, 3)",
    )
    .bind(workspace_id)
    .bind(source_name)
    .execute(pool)
    .await
    .map(|_| ())
}

async fn insert_source_variable_row(
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

async fn insert_source_secret_key_row(
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

async fn delete_source(pool: &PgPool, workspace_id: &str, source_name: &str) {
    sqlx::query("DELETE FROM sources WHERE workspace_id = $1 AND name = $2")
        .bind(workspace_id)
        .bind(source_name)
        .execute(pool)
        .await
        .expect("delete source");
}

async fn delete_workspace(pool: &PgPool, workspace_id: &str) {
    sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(workspace_id)
        .execute(pool)
        .await
        .expect("delete workspace");
}

async fn source_count(pool: &PgPool, workspace_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM sources WHERE workspace_id = $1")
        .bind(workspace_id)
        .fetch_one(pool)
        .await
        .expect("count sources")
}

async fn source_variable_count(pool: &PgPool, workspace_id: &str, source_name: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM source_variables
         WHERE workspace_id = $1 AND source_name = $2",
    )
    .bind(workspace_id)
    .bind(source_name)
    .fetch_one(pool)
    .await
    .expect("count source variables")
}

async fn source_secret_key_count(pool: &PgPool, workspace_id: &str, source_name: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM source_secret_keys
         WHERE workspace_id = $1 AND source_name = $2",
    )
    .bind(workspace_id)
    .bind(source_name)
    .fetch_one(pool)
    .await
    .expect("count source secret keys")
}

async fn open_postgres_pool(database_url: &str) -> PgPool {
    PgPoolOptions::new()
        .connect(database_url)
        .await
        .expect("open Postgres database")
}

#[expect(
    clippy::disallowed_methods,
    reason = "The Postgres integration suite is explicitly gated by this CI/test-only variable."
)]
fn postgres_test_url() -> Option<String> {
    std::env::var("CORAL_TEST_POSTGRES_URL")
        .ok()
        .filter(|value| !value.is_empty())
}
