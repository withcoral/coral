#![expect(
    clippy::indexing_slicing,
    reason = "proto regression assertions intentionally fail loudly in tests"
)]

use coral_api::v1::{
    SearchProvider, SearchProviderState, SearchRequest, TableFunctionKind, ValidateSourceRequest,
    Workspace,
};
use coral_client::default_workspace;
use serde_json::json;
use tonic::{Code, Request};

use super::harness::{GrpcHarness, manifest_yaml};

#[tokio::test]
async fn search_service_returns_structured_shell_response() {
    let harness = GrpcHarness::new().await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "github issue".to_string(),
            limit: 0,
        }))
        .await
        .expect("search")
        .into_inner();

    assert!(response.results.is_empty());
    let truncation = response.truncation.expect("truncation");
    assert!(!truncation.truncated);
    assert_eq!(truncation.returned_count, 0);
    assert_eq!(truncation.max_results, 10);
    assert_eq!(response.provider_statuses.len(), 3);
    assert_eq!(
        SearchProvider::try_from(response.provider_statuses[0].provider).expect("provider"),
        SearchProvider::CatalogMetadata
    );
    assert_eq!(
        SearchProviderState::try_from(response.provider_statuses[0].state).expect("state"),
        SearchProviderState::NotEnabled
    );
    assert_eq!(
        SearchProvider::try_from(response.provider_statuses[1].provider).expect("provider"),
        SearchProvider::ObservedValues
    );
    assert_eq!(
        SearchProviderState::try_from(response.provider_statuses[1].state).expect("state"),
        SearchProviderState::NotEnabled
    );
    assert_eq!(
        SearchProvider::try_from(response.provider_statuses[2].provider).expect("provider"),
        SearchProvider::NativeFanout
    );
    assert_eq!(
        SearchProviderState::try_from(response.provider_statuses[2].state).expect("state"),
        SearchProviderState::NotEnabled
    );
    for status in &response.provider_statuses {
        assert!(
            status.coverage.is_none(),
            "disabled provider coverage should be absent"
        );
    }
}

#[tokio::test]
async fn search_service_rejects_unknown_workspace() {
    let harness = GrpcHarness::new().await;

    let status = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(Workspace {
                name: "missing".to_string(),
            }),
            query: "github issue".to_string(),
            limit: 0,
        }))
        .await
        .expect_err("unknown workspace should fail");

    assert_eq!(status.code(), Code::NotFound);
    assert!(
        status.message().contains("workspace 'missing' not found"),
        "expected workspace not found message, got: {}",
        status.message()
    );
}

#[tokio::test]
async fn search_service_rejects_empty_query() {
    let harness = GrpcHarness::new().await;

    let status = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: " ".to_string(),
            limit: 0,
        }))
        .await
        .expect_err("empty query should fail");

    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status.message().contains("search query must not be empty"),
        "unexpected message: {}",
        status.message()
    );
}

#[tokio::test]
async fn table_function_proto_exposes_search_metadata() {
    let harness = GrpcHarness::new().await;
    let manifest = manifest_yaml(&json!({
        "name": "searchable",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": "https://example.test",
        "functions": [{
            "name": "search_messages",
            "kind": "search",
            "description": "Search messages",
            "args": [{
                "name": "query",
                "required": true,
                "bind": { "arg": "query" }
            }],
            "request": {
                "method": "GET",
                "path": "/messages",
                "query": [{ "name": "q", "from": "arg", "key": "query" }]
            },
            "response": { "rows_path": ["items"] },
            "columns": [{ "name": "title", "type": "Utf8" }],
            "search_limits": {
                "default_top_k": 5,
                "max_top_k": 20,
                "max_calls_per_query": 2
            }
        }]
    }));
    harness
        .import_source(manifest, Vec::new(), Vec::new())
        .await;

    let response = harness
        .source_client()
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name: "searchable".to_string(),
        }))
        .await
        .expect("validate source")
        .into_inner();

    assert_eq!(response.table_functions.len(), 1);
    let function = &response.table_functions[0];
    assert_eq!(
        TableFunctionKind::try_from(function.kind).expect("kind"),
        TableFunctionKind::Search
    );
    let search_limits = function.search_limits.as_ref().expect("search limits");
    assert_eq!(search_limits.default_top_k, 5);
    assert_eq!(search_limits.max_top_k, 20);
    assert_eq!(search_limits.max_calls_per_query, 2);
}
