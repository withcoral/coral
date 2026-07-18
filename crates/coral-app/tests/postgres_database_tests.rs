//! Pins gRPC server startup behavior for configured Postgres storage.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

use std::collections::BTreeMap;
use std::fs;

use coral_api::v1::{
    AddIdentitySpecRequest, GlobalIdentitySpecScope, IdentitySpecInputValue, IdentitySpecScope,
    identity_spec_scope,
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

const ALPHA: &str = "identity-alpha";
const BETA: &str = "identity-beta";

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

    let app = AppClient::connect(server.endpoint_uri())
        .await
        .expect("connect client");
    let fixed = app
        .identity_spec_client()
        .add_identity_spec(Request::new(AddIdentitySpecRequest {
            manifest_yaml: fixed_token_manifest().to_string(),
            input_values: Vec::new(),
            scope: Some(global_scope()),
        }))
        .await
        .expect("install fixed-token identity spec without setup inputs")
        .into_inner();
    assert_eq!(
        fixed.identity_spec.expect("installed identity spec").name,
        "postgres_fixed_token"
    );

    let oauth = app
        .identity_spec_client()
        .add_identity_spec(Request::new(AddIdentitySpecRequest {
            manifest_yaml: oauth_manifest().to_string(),
            input_values: vec![IdentitySpecInputValue {
                key: "CLIENT_SECRET".to_string(),
                value: "not-persisted".to_string(),
            }],
            scope: Some(global_scope()),
        }))
        .await
        .expect("install OAuth identity spec with setup inputs")
        .into_inner();
    assert_eq!(
        oauth.identity_spec.expect("installed identity spec").name,
        "postgres_oauth"
    );
    assert!(
        config_dir
            .join("credentials")
            .join("encryption.key")
            .exists(),
        "identity setup-input persistence should create the local encryption key"
    );

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

fn global_scope() -> IdentitySpecScope {
    IdentitySpecScope {
        value: Some(identity_spec_scope::Value::Global(
            GlobalIdentitySpecScope {},
        )),
    }
}

fn fixed_token_manifest() -> &'static str {
    "kind: identity\nspec_version: 1\nname: postgres_fixed_token\nversion: 1.0.0\nissuer: demo\ntype: fixed_token\naudience: {host: example.com}\n"
}

fn oauth_manifest() -> &'static str {
    "kind: identity\nspec_version: 1\nname: postgres_oauth\nversion: 1.0.0\nissuer: demo\ntype: oauth\naudience: {host: example.com}\ninputs:\n  CLIENT_SECRET: {kind: secret, required: true}\noauth:\n  method:\n    flow: {type: authorization_code, pkce: disabled}\n    redirect_uri: http://127.0.0.1:53682/oauth/callback\n    endpoints: {authorization_url: 'https://example.com/authorize', token_url: 'https://example.com/token'}\n    client:\n      id: {default: demo}\n      secret: {input: CLIENT_SECRET, transport: basic_auth}\n"
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
    assert_postgres_identity_schema(&pool).await;
}

async fn assert_postgres_identity_schema(pool: &sqlx::PgPool) {
    for workspace in [ALPHA, BETA] {
        sqlx::query("INSERT INTO workspaces (id, created_at_unix_nanos) VALUES ($1, 0)")
            .bind(workspace)
            .execute(pool)
            .await
            .expect("seed identity workspace");
    }
    for row in [
        ("user", "local", None, "user-global", None),
        ("workspace", ALPHA, Some(ALPHA), "workspace-global", None),
        (
            "workspace",
            ALPHA,
            Some(ALPHA),
            "workspace-scoped",
            Some(ALPHA),
        ),
    ] {
        insert_postgres_identity(pool, row)
            .await
            .expect("valid Postgres identity row");
    }
    assert_postgres_identity_rejects_invalid_rows(pool).await;
    sqlx::query("DELETE FROM workspaces WHERE id = 'identity-alpha'")
        .execute(pool)
        .await
        .expect("delete Postgres workspace");
    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM identities")
        .fetch_one(pool)
        .await
        .expect("count cascaded Postgres identity rows");
    assert_eq!(remaining, 1);
}

async fn assert_postgres_identity_rejects_invalid_rows(pool: &sqlx::PgPool) {
    for row in [
        ("unknown", "member", None, "unknown-owner", None),
        ("user", "member", Some(ALPHA), "user-workspace", None),
        ("workspace", ALPHA, None, "missing-workspace", None),
        ("user", "member", None, "user-scoped", Some(ALPHA)),
        (
            "workspace",
            ALPHA,
            Some(ALPHA),
            "cross-workspace",
            Some(BETA),
        ),
        ("workspace", ALPHA, Some(BETA), "owner-mismatch", None),
        ("workspace", "missing", Some("missing"), "missing-row", None),
    ] {
        insert_postgres_identity(pool, row)
            .await
            .expect_err("invalid Postgres identity row must be rejected");
    }
}

type IdentityRow<'a> = (&'a str, &'a str, Option<&'a str>, &'a str, Option<&'a str>);

async fn insert_postgres_identity(
    pool: &sqlx::PgPool,
    (owner_kind, owner_key, workspace_id, name, identity_spec_workspace_id): IdentityRow<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO identities (
            owner_kind, owner_key, workspace_id, name,
            identity_spec_workspace_id, identity_spec_name,
            identity_spec_fingerprint, issuer, identity_type,
            created_at_unix_nanos, updated_at_unix_nanos
         ) VALUES ($1, $2, $3, $4, $5, 'missing-spec', 'fingerprint', 'issuer', 'fixed_token', 1, 1)",
    )
    .bind(owner_kind)
    .bind(owner_key)
    .bind(workspace_id)
    .bind(name)
    .bind(identity_spec_workspace_id)
    .execute(pool)
    .await
    .map(|_| ())
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
