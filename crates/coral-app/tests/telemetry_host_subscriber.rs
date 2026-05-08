//! Pins the host-owned subscriber policy.
//!
//! `init_tracing` installs coral-app's own tracing subscriber as a side effect
//! of `ServerBuilder::start`. When the host process has already installed a
//! global tracing subscriber, coral-app does not overwrite it and does not
//! fail startup — telemetry init becomes a no-op (an explanatory warning is
//! logged through the host's subscriber) and the gRPC server bootstraps
//! normally.
//!
//! This test must live in its own integration test binary: `init_tracing`
//! caches its outcome in a process-global `OnceLock`, so co-locating this
//! scenario with tests that perform a normal startup would let the cached
//! success short-circuit `try_init` and hide the conflict path.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

use coral_api::v1::ListSourcesRequest;
use coral_client::{AppClient, default_workspace, local::ServerBuilder};
use tempfile::TempDir;
use tonic::Request;
use tracing_subscriber::util::SubscriberInitExt as _;

#[tokio::test]
async fn host_subscriber_does_not_block_server_startup() {
    tracing_subscriber::registry()
        .try_init()
        .expect("install host subscriber once per test process");

    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");

    let server = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .start()
        .await
        .expect("start server with host-owned subscriber");
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
