#![allow(
    clippy::indexing_slicing,
    reason = "Proto regression assertions intentionally fail loudly in tests."
)]

use coral_api::v1::search_result::Payload;
use coral_api::v1::{
    DeleteSourceRequest, SearchFieldRole, SearchProvider, SearchProviderState, SearchRequest,
    SearchResult, SearchSurfaceKind, SourceSecret, TableFunctionKind, catalog_item,
};
use coral_client::default_workspace;
use tantivy::collector::Count;
use tantivy::query::{BooleanQuery, Occur, TermQuery};
use tantivy::schema::IndexRecordOption;
use tantivy::{Index, Term};
use tonic::Request;

use super::harness::{
    GrpcHarness, fixture_manifest_with_canonical_table_ranking_yaml,
    fixture_manifest_with_column_preview_yaml, fixture_manifest_with_functions_yaml,
    fixture_manifest_with_inputs_yaml, fixture_manifest_with_many_matching_columns_yaml,
    fixture_manifest_with_test_queries_yaml, source_dir,
};

#[tokio::test]
async fn search_returns_typed_metadata_and_native_search_results() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_functions_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "issue search title".to_string(),
            limit: 50,
        }))
        .await
        .expect("search")
        .into_inner();

    assert_provider_state(
        &response,
        SearchProvider::CatalogMetadata,
        SearchProviderState::ResultsFound,
    );
    assert_provider_state(
        &response,
        SearchProvider::ObservedValues,
        SearchProviderState::Empty,
    );
    assert!(!response.truncation.expect("truncation").truncated);
    assert!(
        harness
            .config_dir()
            .join("workspaces")
            .join("default")
            .join("search")
            .join("tantivy")
            .exists(),
        "search should create the workspace Tantivy search index"
    );
    assert!(response.results.iter().any(|result| result.provider
        == SearchProvider::CatalogMetadata as i32
        && matches!(
            result.payload.as_ref(),
            Some(Payload::NativeSearchPath(path))
                if path
                    .table_function
                    .as_ref()
                    .is_some_and(|function| function.name == "search_issues")
                    && path.sql_call_example.contains("\"searchy\".\"search_issues\"")
                    && path.sql_call_example.contains("\"q\" => '<q>'")
        )));
    assert!(response.results.iter().any(|result| result.provider
        == SearchProvider::CatalogMetadata as i32
        && matches!(
            result.payload.as_ref(),
            Some(Payload::CatalogMetadata(metadata))
                if metadata.item.as_ref().and_then(|item| item.item.as_ref()).is_some_and(|item| match item {
                    coral_api::v1::catalog_item::Item::TableFunction(function) =>
                        function.name == "search_issues" && function.kind() == TableFunctionKind::Search,
                    coral_api::v1::catalog_item::Item::Table(_) => false,
                })
        )));
    assert!(response.results.iter().any(|result| result.provider
        == SearchProvider::CatalogMetadata as i32
        && matches!(
            result.payload.as_ref(),
            Some(Payload::ColumnHint(hint))
                if hint.surface_name == "search_issues"
                    && hint.surface_kind == SearchSurfaceKind::TableFunction as i32
                    && hint.field_role == SearchFieldRole::TableFunctionResultColumn as i32
                    && hint.name == "title"
        )));
}

#[tokio::test]
async fn search_catalog_metadata_includes_compact_table_column_preview() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_column_preview_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "needle".to_string(),
            limit: 10,
        }))
        .await
        .expect("search")
        .into_inner();

    let metadata = response
        .results
        .iter()
        .find_map(catalog_metadata_table)
        .expect("catalog table metadata result");
    assert_eq!(metadata.0.name, "records");
    assert!(
        metadata
            .1
            .matched_fields
            .iter()
            .any(|field| field == "columns")
    );
    let preview = metadata
        .1
        .table_column_preview
        .as_ref()
        .expect("table column preview");
    assert_eq!(preview.column_count, 11);
    assert_eq!(preview.columns.len(), 8);
    assert_eq!(preview.omitted_column_count, 3);
    assert_eq!(
        preview
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "owner", "repo", "id", "title", "status", "state", "html_url", "body"
        ]
    );
    assert!(preview.columns[0].is_required_filter);
    assert!(preview.columns[1].is_required_filter);
    let body = preview
        .columns
        .iter()
        .find(|column| column.name == "body")
        .expect("matched body column");
    assert!(
        body.matched_fields
            .iter()
            .any(|field| field == "description")
    );
}

#[tokio::test]
async fn search_marks_catalog_partial_without_response_truncation_for_raw_index_overflow() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_many_matching_columns_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "needle".to_string(),
            limit: 10,
        }))
        .await
        .expect("search")
        .into_inner();

    assert_provider_state(
        &response,
        SearchProvider::CatalogMetadata,
        SearchProviderState::Partial,
    );
    let truncation = response.truncation.expect("truncation");
    assert!(!truncation.truncated);
    assert!(response.results.len() <= 10);
}

#[tokio::test]
async fn search_catalog_metadata_loads_sources_with_stored_secrets() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_inputs_yaml(),
            Vec::new(),
            vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "secret-token".to_string(),
            }],
        )
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "secured messages".to_string(),
            limit: 10,
        }))
        .await
        .expect("search")
        .into_inner();

    assert_provider_state(
        &response,
        SearchProvider::CatalogMetadata,
        SearchProviderState::ResultsFound,
    );
    let metadata = response
        .results
        .iter()
        .find_map(catalog_metadata_table)
        .expect("secured table metadata result");
    assert_eq!(metadata.0.schema_name, "secured_messages");
    assert_eq!(metadata.0.name, "messages");
}

#[tokio::test]
async fn search_fails_when_a_stored_source_cannot_load() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_inputs_yaml(),
            Vec::new(),
            vec![SourceSecret {
                key: "API_TOKEN".to_string(),
                value: "secret-token".to_string(),
            }],
        )
        .await;
    std::fs::remove_file(source_dir(harness.config_dir(), "secured_messages").join("secrets.env"))
        .expect("remove stored secret material");

    let error = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "secured messages".to_string(),
            limit: 10,
        }))
        .await
        .expect_err("search should fail instead of indexing a partial catalog");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("secured_messages"));
    assert!(error.message().contains("stored catalog"));
}

#[tokio::test]
async fn search_ranks_canonical_same_source_table_before_incidental_matches() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_canonical_table_ranking_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "github pull request pr".to_string(),
            limit: 10,
        }))
        .await
        .expect("search")
        .into_inner();

    let first_catalog_table = response
        .results
        .iter()
        .find_map(catalog_metadata_table)
        .map(|(table, _metadata)| table.name.as_str())
        .expect("catalog table result");
    assert_eq!(first_catalog_table, "pulls");
}

#[tokio::test]
async fn search_rejects_empty_and_too_large_queries() {
    let harness = GrpcHarness::new().await;

    let empty = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "   ".to_string(),
            limit: 10,
        }))
        .await
        .expect_err("empty query should fail");
    assert_eq!(empty.code(), tonic::Code::InvalidArgument);
    assert!(empty.message().contains("query"));

    let too_long = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "a".repeat(513),
            limit: 10,
        }))
        .await
        .expect_err("long query should fail");
    assert_eq!(too_long.code(), tonic::Code::InvalidArgument);
    assert!(too_long.message().contains("at most 512 bytes"));
}

#[tokio::test]
async fn search_returns_observed_values_after_successful_sql() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_test_queries_yaml(harness.temp_path(), &[]),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let rows = harness
        .execute_sql_rows("SELECT text FROM local_messages.messages WHERE text = 'hello'")
        .await;
    assert_eq!(rows.len(), 1);

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "hello".to_string(),
            limit: 10,
        }))
        .await
        .expect("search")
        .into_inner();

    assert_provider_state(
        &response,
        SearchProvider::ObservedValues,
        SearchProviderState::ResultsFound,
    );
    assert!(response.results.iter().any(|result| result.provider
        == SearchProvider::ObservedValues as i32
        && matches!(
            result.payload.as_ref(),
            Some(Payload::ObservedValue(value))
                if value.schema_name == "local_messages"
                    && value.surface_name == "messages"
                    && value.surface_kind == SearchSurfaceKind::Table as i32
                    && value.column_name == "text"
                    && value.field_path == "text"
                    && value.value == "hello"
                    && value.observed_count == 1
                    && !value.last_observed_at.is_empty()
        )));
}

#[tokio::test]
async fn search_does_not_mark_observed_values_partial_for_pending_queue_work() {
    let temp = tempfile::tempdir().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    std::fs::create_dir_all(&config_dir).expect("create config dir");
    std::fs::write(
        config_dir.join("config.toml"),
        "[search]\nobserved_queue_foreground_drain_ms = 0\n",
    )
    .expect("write config");
    let harness = GrpcHarness::start_with_config_dir(config_dir).await;
    harness
        .import_source(
            fixture_manifest_with_test_queries_yaml(harness.temp_path(), &[]),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let rows = harness
        .execute_sql_rows("SELECT text FROM local_messages.messages WHERE text = 'hello'")
        .await;
    assert_eq!(rows.len(), 1);

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "hello".to_string(),
            limit: 10,
        }))
        .await
        .expect("search")
        .into_inner();

    assert_provider_state(
        &response,
        SearchProvider::ObservedValues,
        SearchProviderState::Empty,
    );
    let observed_status = response
        .provider_statuses
        .iter()
        .find(|status| status.provider == SearchProvider::ObservedValues as i32)
        .expect("observed values provider status");
    assert!(observed_status.note.contains("queued indexing jobs remain"));
    assert!(
        !response
            .results
            .iter()
            .any(|result| result.provider == SearchProvider::ObservedValues as i32)
    );
}

#[tokio::test]
async fn search_reports_observed_storage_budget_exhaustion() {
    let temp = tempfile::tempdir().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    std::fs::create_dir_all(&config_dir).expect("create config dir");
    std::fs::write(
        config_dir.join("config.toml"),
        "[search]\nobserved_max_storage_mb = 0\n",
    )
    .expect("write config");
    let harness = GrpcHarness::start_with_config_dir(config_dir).await;
    harness
        .import_source(
            fixture_manifest_with_test_queries_yaml(harness.temp_path(), &[]),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "hello".to_string(),
            limit: 10,
        }))
        .await
        .expect("search")
        .into_inner();

    assert_provider_state(
        &response,
        SearchProvider::ObservedValues,
        SearchProviderState::Empty,
    );
    let observed_status = response
        .provider_statuses
        .iter()
        .find(|status| status.provider == SearchProvider::ObservedValues as i32)
        .expect("observed values provider status");
    assert!(
        observed_status
            .note
            .contains("observed-value indexing is paused because the storage budget is exhausted")
    );
}

#[tokio::test]
async fn search_applies_limit_and_reports_truncation() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_functions_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "issue".to_string(),
            limit: 1,
        }))
        .await
        .expect("search")
        .into_inner();
    let truncation = response.truncation.expect("truncation");

    assert_eq!(response.results.len(), 1);
    assert!(truncation.truncated);
    assert_eq!(truncation.returned_count, 1);
    assert_eq!(truncation.max_results, 1);
}

#[tokio::test]
async fn search_uses_default_limit_when_request_limit_is_zero() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            fixture_manifest_with_functions_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "issue".to_string(),
            limit: 0,
        }))
        .await
        .expect("search")
        .into_inner();
    let truncation = response.truncation.expect("truncation");

    assert_eq!(truncation.max_results, 50);
}

#[tokio::test]
async fn search_rejects_limits_above_maximum() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "issue".to_string(),
            limit: 101,
        }))
        .await
        .expect_err("limit above maximum should fail");

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("100"));
}

#[tokio::test]
async fn source_mutations_mark_catalog_search_index_dirty() {
    let harness = GrpcHarness::new().await;
    let manifest_yaml = fixture_manifest_with_functions_yaml();
    harness
        .import_source(manifest_yaml.clone(), Vec::new(), Vec::new())
        .await;

    assert!(!search_index_path(&harness).exists());
    search(&harness, "issue search title").await;
    assert!(catalog_entity_count(&harness, "search_issues") > 0);

    let updated_manifest_yaml = manifest_yaml.replace("search_issues", "search_tasks");
    harness
        .import_source(updated_manifest_yaml, Vec::new(), Vec::new())
        .await;
    assert!(catalog_entity_count(&harness, "search_issues") > 0);
    assert_eq!(catalog_entity_count(&harness, "search_tasks"), 0);

    search(&harness, "task search title").await;
    assert_eq!(catalog_entity_count(&harness, "search_issues"), 0);
    assert!(catalog_entity_count(&harness, "search_tasks") > 0);

    harness
        .source_client()
        .delete_source(Request::new(DeleteSourceRequest {
            workspace: Some(default_workspace()),
            name: "searchy".to_string(),
        }))
        .await
        .expect("delete source");
    assert!(total_catalog_entity_count(&harness) > 0);

    search(&harness, "task search title").await;
    assert_eq!(catalog_entity_count(&harness, "placeholder"), 0);
    assert_eq!(catalog_entity_count(&harness, "lookup_issue"), 0);
    assert_eq!(catalog_entity_count(&harness, "search_issues"), 0);
    assert_eq!(catalog_entity_count(&harness, "search_tasks"), 0);
}

async fn search(harness: &GrpcHarness, query: &str) {
    harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: query.to_string(),
            limit: 10,
        }))
        .await
        .expect("search");
}

fn assert_provider_state(
    response: &coral_api::v1::SearchResponse,
    provider: SearchProvider,
    state: SearchProviderState,
) {
    let status = response
        .provider_statuses
        .iter()
        .find(|status| status.provider == provider as i32)
        .expect("provider status");
    assert_eq!(status.state, state as i32);
}

fn catalog_metadata_table(
    result: &SearchResult,
) -> Option<(
    &coral_api::v1::TableSummary,
    &coral_api::v1::CatalogMetadata,
)> {
    let Payload::CatalogMetadata(metadata) = result.payload.as_ref()? else {
        return None;
    };
    let catalog_item::Item::Table(table) = metadata.item.as_ref()?.item.as_ref()? else {
        return None;
    };
    Some((table, metadata))
}

fn catalog_entity_count(harness: &GrpcHarness, surface_name: &str) -> u32 {
    catalog_count_for_terms(harness, vec![("surface_name", surface_name)])
}

fn total_catalog_entity_count(harness: &GrpcHarness) -> u32 {
    catalog_count_for_terms(harness, Vec::new())
}

fn catalog_count_for_terms(harness: &GrpcHarness, terms: Vec<(&str, &str)>) -> u32 {
    let index = Index::open_in_dir(search_index_path(harness)).expect("open search index");
    let schema = index.schema();
    let entity_kind = schema.get_field("entity_kind").expect("entity_kind field");
    let mut clauses = vec![(
        Occur::Must,
        Box::new(TermQuery::new(
            Term::from_field_text(entity_kind, "catalog"),
            IndexRecordOption::Basic,
        )) as Box<dyn tantivy::query::Query>,
    )];
    for (field_name, value) in terms {
        let field = schema.get_field(field_name).expect("field");
        clauses.push((
            Occur::Must,
            Box::new(TermQuery::new(
                Term::from_field_text(field, value),
                IndexRecordOption::Basic,
            )),
        ));
    }
    let query = BooleanQuery::new(clauses);
    let reader = index.reader().expect("reader");
    let searcher = reader.searcher();
    let count = searcher.search(&query, &Count).expect("catalog count");
    u32::try_from(count).expect("count fits u32")
}

fn search_index_path(harness: &GrpcHarness) -> std::path::PathBuf {
    harness
        .config_dir()
        .join("workspaces")
        .join("default")
        .join("search")
        .join("tantivy")
}
