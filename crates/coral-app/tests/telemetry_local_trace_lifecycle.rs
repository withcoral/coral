//! Pins local trace API registration to the process-owned trace exporter.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

use std::path::{Path, PathBuf};

use coral_api::v1::ListTracesRequest;
use coral_api::v1::trace_service_client::TraceServiceClient;
use coral_client::local::ServerBuilder;
use serde_json::json;
use tempfile::TempDir;
use tonic::Request;
use tonic::transport::Endpoint;

#[tokio::test]
async fn repeated_starts_read_the_process_local_trace_store() {
    let temp = TempDir::new().expect("temp dir");
    let first_config_dir = temp.path().join("first-config");
    let second_config_dir = temp.path().join("second-config");
    enable_local_tracing(&first_config_dir);
    enable_local_tracing(&second_config_dir);

    let first_server = ServerBuilder::new()
        .with_config_dir(&first_config_dir)
        .start()
        .await
        .expect("start first server");
    write_trace(&trace_store_dir(&first_config_dir), "first-store-trace");
    write_trace(&trace_store_dir(&second_config_dir), "second-store-trace");
    first_server
        .shutdown()
        .await
        .expect("shutdown first server");

    let second_server = ServerBuilder::new()
        .with_config_dir(&second_config_dir)
        .start()
        .await
        .expect("start second server");
    let trace_ids = list_trace_ids(second_server.endpoint_uri()).await;

    assert!(
        trace_ids
            .iter()
            .any(|trace_id| trace_id == "first-store-trace"),
        "second server should read from the process-local trace store: {trace_ids:?}"
    );
    assert!(
        trace_ids
            .iter()
            .all(|trace_id| trace_id != "second-store-trace"),
        "second server should not read from its own unwritten trace store: {trace_ids:?}"
    );

    second_server
        .shutdown()
        .await
        .expect("shutdown second server");
}

fn enable_local_tracing(config_dir: &Path) {
    std::fs::create_dir_all(config_dir).expect("create config dir");
    std::fs::write(
        config_dir.join("config.toml"),
        r"
version = 1

[trace_history]
enabled = true
",
    )
    .expect("write telemetry config");
}

fn trace_store_dir(config_dir: &Path) -> PathBuf {
    config_dir.join("telemetry").join("traces")
}

fn write_trace(dir: &Path, trace_id: &str) {
    std::fs::create_dir_all(dir).expect("create trace dir");
    let record = json!({
        "trace_id": trace_id,
        "span_id": "span-1",
        "parent_span_id": null,
        "parent_span_is_remote": false,
        "name": "coral.query",
        "kind": "internal",
        "status": "ok",
        "status_message": null,
        "start_time_unix_nanos": 1,
        "end_time_unix_nanos": 2,
        "duration_nanos": 1,
        "attributes_json": "{}",
        "events_json": "[]",
        "links_json": "[]",
        "resource_json": "{}",
        "scope_name": "test",
        "scope_version": null,
        "scope_schema_url": null,
        "scope_attributes_json": "{}",
        "trace_flags": 0,
        "trace_state": "",
        "is_remote": false
    });
    std::fs::write(dir.join("spans.jsonl"), format!("{record}\n")).expect("write trace");
}

async fn list_trace_ids(endpoint_uri: &str) -> Vec<String> {
    let channel = Endpoint::from_shared(endpoint_uri.to_string())
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    TraceServiceClient::new(channel)
        .list_traces(Request::new(ListTracesRequest {
            page_size: 10,
            page_token: String::new(),
        }))
        .await
        .expect("list traces")
        .into_inner()
        .traces
        .into_iter()
        .map(|trace| trace.trace_id)
        .collect()
}
