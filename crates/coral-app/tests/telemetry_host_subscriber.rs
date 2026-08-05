//! Pins the host-owned subscriber startup and local-only attribute policies.
//!
//! `init_tracing` installs coral-app's own tracing subscriber as a side effect
//! of `ServerBuilder::start`. When the host process has already installed a
//! global tracing subscriber, coral-app does not overwrite it and does not
//! fail startup — telemetry init becomes a no-op (an explanatory warning is
//! logged through the host's subscriber) and the gRPC server bootstraps
//! normally.
//!
//! A host-owned subscriber still receives Coral's public spans, but Coral must
//! not write local-only attributes into it because the host owns that
//! subscriber's storage, retention, and deletion behavior.
//!
//! This test must live in its own integration test binary: `init_tracing`
//! caches its outcome in a process-global `OnceLock`, so co-locating this
//! scenario with tests that perform a normal startup would let the cached
//! success short-circuit `try_init` and hide the conflict path.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

use coral_api::v1::trace_service_client::TraceServiceClient;
use coral_api::v1::{ListSourcesRequest, ListTracesRequest, SearchRequest};
use coral_client::{AppClient, default_workspace, local::ServerBuilder};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use tempfile::TempDir;
use tonic::transport::Endpoint;
use tonic::{Code, Request};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

const SEARCH_SENTINEL: &str = "HOST_SUBSCRIBER_LOCAL_SEARCH_SENTINEL";

#[tokio::test]
async fn host_subscriber_keeps_server_available_without_local_only_attributes() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("host-subscriber-privacy-test");
    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
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

    app.search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: SEARCH_SENTINEL.to_string(),
            limit: 0,
        }))
        .await
        .expect("search empty catalog");

    provider.force_flush().expect("flush host spans");
    let spans = exporter.get_finished_spans().expect("finished host spans");
    assert!(
        spans.iter().any(|span| span.name == "coral.search"),
        "the host subscriber should still receive the public Search span"
    );
    assert!(spans.iter().all(|span| {
        span.attributes.iter().all(|attribute| {
            !attribute
                .key
                .as_str()
                .starts_with(coral_telemetry::LOCAL_ONLY_SPAN_ATTRIBUTE_PREFIX)
        })
    }));
    assert!(!format!("{spans:?}").contains(SEARCH_SENTINEL));

    let channel = Endpoint::from_shared(server.endpoint_uri().to_string())
        .expect("endpoint")
        .connect()
        .await
        .expect("connect trace client");
    let trace_status = TraceServiceClient::new(channel)
        .list_traces(Request::new(ListTracesRequest {
            page_size: 10,
            page_token: String::new(),
            workspace: None,
        }))
        .await
        .expect_err("host-owned subscriber should leave trace service disabled");
    assert_eq!(trace_status.code(), Code::Unimplemented);

    server.shutdown().await.expect("shutdown server");
    provider.shutdown().expect("shutdown host provider");
}
