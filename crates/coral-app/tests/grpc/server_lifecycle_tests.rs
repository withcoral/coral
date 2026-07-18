//! Pins the server lifecycle contract.
//!
//! `ServerBuilder::start` and `RunningServer::shutdown` describe the local
//! gRPC server lifecycle and may be invoked repeatedly within a single
//! process. Telemetry is process-scoped: it is initialized once via
//! `OnceLock` and flushed by the owning binary or test harness via
//! `coral_app::shutdown_tracing` at process exit.

use std::fs;

use coral_api::v1::ListSourcesRequest;
use coral_client::{
    AppClient, default_workspace,
    local::{LocalServerError, ServerBuilder},
};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;
use tonic::Request;

#[tokio::test]
async fn server_lifecycle_can_repeat_within_process() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");

    for _ in 0..2 {
        let server = ServerBuilder::new()
            .with_config_dir(&config_dir)
            .start()
            .await
            .expect("start server");
        assert_default_sqlite_db_is_migrated(&config_dir).await;
        let app = AppClient::connect(server.endpoint_uri())
            .await
            .expect("connect client");

        let sources = app
            .source_client()
            .list_sources(Request::new(ListSourcesRequest {
                workspace: Some(default_workspace()),
            }))
            .await
            .expect("list sources")
            .into_inner()
            .sources;
        assert!(sources.is_empty());

        server.shutdown().await.expect("shutdown server");
    }
}

#[tokio::test]
async fn server_lifecycle_rejects_postgres_config_without_url_env_value() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let missing_url_env = format!(
        "CORAL_TEST_POSTGRES_URL_MISSING_FOR_SERVER_START_{}",
        uuid::Uuid::new_v4()
            .simple()
            .to_string()
            .to_ascii_uppercase()
    );
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        format!("[database]\nbackend = \"postgres\"\nurl_env = \"{missing_url_env}\"\n"),
    )
    .expect("write config");

    let result = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .start()
        .await;
    let error = match result {
        Err(error) => error,
        Ok(server) => {
            server.shutdown().await.expect("shutdown unexpected server");
            panic!("server start should fail without configured Postgres URL env var");
        }
    };

    match error {
        LocalServerError::FailedPrecondition(detail) => assert!(
            detail.contains(&missing_url_env),
            "unexpected detail: {detail}"
        ),
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn server_lifecycle_validates_configured_encryption_key_before_opening_database() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let missing_key_env = format!(
        "CORAL_TEST_CREDENTIAL_KEY_MISSING_FOR_SERVER_START_{}",
        uuid::Uuid::new_v4()
            .simple()
            .to_string()
            .to_ascii_uppercase()
    );
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        format!("[encryption]\nencryption_key_env = \"{missing_key_env}\"\n"),
    )
    .expect("write config");

    let result = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .start()
        .await;
    let error = match result {
        Err(error) => error,
        Ok(server) => {
            server.shutdown().await.expect("shutdown unexpected server");
            panic!("server start should fail without the configured encryption key");
        }
    };

    match error {
        LocalServerError::FailedPrecondition(detail) => assert!(
            detail.contains(&missing_key_env),
            "unexpected detail: {detail}"
        ),
        other => panic!("unexpected error: {other}"),
    }
    assert!(
        !config_dir.join("coral.db").exists(),
        "credential-key validation must happen before the database is opened"
    );
}

async fn assert_default_sqlite_db_is_migrated(config_dir: &std::path::Path) {
    let database_file = config_dir.join("coral.db");
    assert!(
        database_file.exists(),
        "default SQLite database should exist"
    );

    let options = SqliteConnectOptions::new().filename(&database_file);
    let pool = SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .expect("open created SQLite database");
    let table_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'workspaces'",
    )
    .fetch_one(&pool)
    .await
    .expect("inspect migrated schema");
    assert_eq!(table_count, 1, "workspaces table should be migrated");
}
