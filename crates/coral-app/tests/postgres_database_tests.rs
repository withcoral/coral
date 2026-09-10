//! Pins gRPC server startup behavior for configured Postgres storage.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

use std::collections::BTreeMap;
use std::fs;

use coral_api::v1::{
    CreateWorkspaceRequest, DeleteWorkspaceRequest, SearchProvider, SearchProviderState,
    SearchRequest, Workspace,
};
use coral_client::AppClient;
use coral_client::local::ServerBuilder;
use coral_engine::{
    CoralQuery, QueryRuntimeConfig, QuerySource, RuntimeSourceComponent, RuntimeSourcePackage,
};
use coral_spec::{
    DatabaseConnectionSpec, DatabaseSourceManifest, ParsedTemplate, PostgresConnectionSpec,
    SourceManifestCommon,
};
use sqlx::postgres::PgPoolOptions;
use tempfile::TempDir;
use tonic::Request;

#[tokio::test]
#[ignore = "set CORAL_TEST_POSTGRES_URL to run Postgres catalog search end to end"]
async fn server_serves_catalog_search_from_postgres_and_removes_deleted_workspace_state() {
    let Some(database_url) = postgres_test_url() else {
        return;
    };
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        "[database]\nbackend = \"postgres\"\nurl_env = \"CORAL_TEST_POSTGRES_URL\"\n\n[search]\nbackend = \"postgres\"\n",
    )
    .expect("write config");
    let pool = PgPoolOptions::new()
        .connect(&database_url)
        .await
        .expect("open Postgres database");

    let server = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .start()
        .await
        .expect("start server with Postgres search config");
    let registry_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('search_registry.workspaces') IS NOT NULL")
            .fetch_one(&pool)
            .await
            .expect("probe search registry");
    assert!(registry_exists, "boot must bootstrap the search registry");
    assert!(
        !config_dir.join("workspaces").exists()
            || !any_sqlite_search_file(&config_dir.join("workspaces")),
        "no SQLite search sidecar may exist with the Postgres backend"
    );

    let app = AppClient::connect(server.endpoint_uri())
        .await
        .expect("connect client");
    let workspace_name = format!("usp-e2e-{}", uuid::Uuid::new_v4().simple());
    let workspace = Workspace {
        name: workspace_name.clone(),
    };
    app.workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace.clone()),
        }))
        .await
        .expect("create workspace");

    assert_search_reports_postgres_provider_statuses(&app, &workspace).await;

    let surrogate_id = registry_surrogate_id(&pool, &workspace_name)
        .await
        .expect("the first search registers the workspace");
    let schema = format!("search_ws_{surrogate_id}");
    assert!(
        postgres_schema_exists(&pool, &schema).await,
        "schema {schema} must exist after the first search"
    );

    app.workspace_client()
        .delete_workspace(Request::new(DeleteWorkspaceRequest {
            workspace: Some(workspace),
        }))
        .await
        .expect("delete workspace");
    assert_eq!(
        registry_surrogate_id(&pool, &workspace_name).await,
        None,
        "deletion must remove the registry row"
    );
    assert!(
        !postgres_schema_exists(&pool, &schema).await,
        "deletion must drop schema {schema}"
    );

    server.shutdown().await.expect("shutdown server");
}

/// Catalog search on an empty Workspace answers `Empty`, and observed values
/// report `not_enabled` naming the backend rather than erroring.
async fn assert_search_reports_postgres_provider_statuses(app: &AppClient, workspace: &Workspace) {
    let response = app
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(workspace.clone()),
            query: "benchmark".to_string(),
            limit: 0,
        }))
        .await
        .expect("search")
        .into_inner();
    assert_eq!(response.provider_statuses.len(), 3);
    let status = |provider: SearchProvider| {
        response
            .provider_statuses
            .iter()
            .find(|status| status.provider == provider as i32)
            .unwrap_or_else(|| panic!("missing status for {provider:?}"))
    };
    assert_eq!(
        status(SearchProvider::CatalogMetadata).state,
        SearchProviderState::Empty as i32,
        "an empty catalog is served, not errored: {:?}",
        status(SearchProvider::CatalogMetadata)
    );
    let observed = status(SearchProvider::ObservedValues);
    assert_eq!(observed.state, SearchProviderState::NotEnabled as i32);
    assert!(
        observed
            .note
            .contains("not available on the postgres search backend"),
        "unexpected observed note: {}",
        observed.note
    );
}

async fn registry_surrogate_id(pool: &sqlx::PgPool, workspace_name: &str) -> Option<i64> {
    sqlx::query_scalar(
        "SELECT surrogate_id FROM search_registry.workspaces WHERE workspace_name = $1",
    )
    .bind(workspace_name)
    .fetch_optional(pool)
    .await
    .expect("read registry row")
}

fn any_sqlite_search_file(root: &std::path::Path) -> bool {
    fs::read_dir(root).is_ok_and(|entries| {
        entries.flatten().any(|entry| {
            let path = entry.path();
            if path.is_dir() {
                any_sqlite_search_file(&path)
            } else {
                path.extension()
                    .is_some_and(|extension| extension == "sqlite3")
            }
        })
    })
}

async fn postgres_schema_exists(pool: &sqlx::PgPool, schema: &str) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)")
        .bind(schema)
        .fetch_one(pool)
        .await
        .expect("probe schema")
}

#[tokio::test]
#[ignore = "set CORAL_TEST_POSTGRES_URL to run configured Postgres startup coverage"]
async fn server_lifecycle_can_start_with_postgres_database_config() {
    let Some(database_url) = postgres_test_url() else {
        return;
    };
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        "[database]\nbackend = \"postgres\"\nurl_env = \"CORAL_TEST_POSTGRES_URL\"\n",
    )
    .expect("write config");

    let server = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .start()
        .await
        .expect("start server with Postgres config");
    assert_postgres_db_is_migrated(&database_url).await;

    server.shutdown().await.expect("shutdown server");
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

async fn assert_postgres_db_is_migrated(database_url: &str) {
    let pool = PgPoolOptions::new()
        .connect(database_url)
        .await
        .expect("open Postgres database");
    let table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('public.workspaces') IS NOT NULL")
            .fetch_one(&pool)
            .await
            .expect("inspect migrated Postgres schema");
    assert!(table_exists, "workspaces table should be migrated");
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
