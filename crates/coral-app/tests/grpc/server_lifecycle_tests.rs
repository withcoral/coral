//! Pins the server lifecycle contract.
//!
//! `ServerBuilder::start` and `RunningServer::shutdown` describe the local
//! gRPC server lifecycle and may be invoked repeatedly within a single
//! process. Telemetry is process-scoped: it is initialized once via
//! `OnceLock` and flushed by the owning binary or test harness via
//! `coral_app::shutdown_tracing` at process exit.

use coral_api::v1::ListSourcesRequest;
use coral_client::{AppClient, default_workspace, local::ServerBuilder};
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
