//! Pins gRPC server startup behavior for configured Postgres storage.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

use std::fs;

use coral_api::v1::ListUserOwnedIdentitiesRequest;
use coral_client::{AppClient, local::ServerBuilder};
use sqlx::postgres::PgPoolOptions;
use tempfile::TempDir;
use tonic::Request;

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
    let app = AppClient::connect(server.endpoint_uri())
        .await
        .expect("connect Postgres-backed client");
    let identities = app
        .identity_client()
        .list_user_owned_identities(Request::new(ListUserOwnedIdentitiesRequest {}))
        .await
        .expect("keyless Postgres supports safe identity reads")
        .into_inner();
    assert!(identities.identities.is_empty());
    assert_postgres_db_is_migrated(&database_url).await;

    server.shutdown().await.expect("shutdown server");
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
