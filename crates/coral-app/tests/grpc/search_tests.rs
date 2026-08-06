#![expect(
    clippy::indexing_slicing,
    reason = "proto regression assertions intentionally fail loudly in tests"
)]

use std::fs;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use coral_api::v1::{
    AddFunctionRequest, CatalogItem, CatalogItemKind, ClearSearchDataRequest,
    CreateWorkspaceRequest, DeleteWorkspaceRequest, DrainSearchQueueRequest, ListCatalogRequest,
    PaginationRequest, RebuildSearchIndexRequest, SearchClearTarget, SearchDataScope,
    SearchIndexProvider, SearchMaintenanceState, SearchProvider, SearchProviderState,
    SearchRequest, SearchResult as ProtoSearchResult, SearchSurfaceRef, TableFunctionKind,
    ValidateSourceRequest, Workspace, catalog_item, search_clear_target, search_maintenance_result,
    search_result,
};
use coral_app::EngineExtensionsProvider;
use coral_client::default_workspace;
use coral_engine::{
    EngineExtensions, QuerySource, SourceDecorator, SourceDecoratorError, SourceTables,
};
use serde_json::json;
use tonic::{Code, Request};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::harness::{GrpcHarness, manifest_yaml, source_dir};

// The old live-scope VALUES CTE bound five fields per surface. At 6,553 surfaces, the
// count query's two additional fields exceeded bundled SQLite's 32,766-variable limit.
const SQLITE_VARIABLE_LIMIT_REGRESSION_SURFACE_COUNT: usize = 6_553;

fn workspace(name: &str) -> Workspace {
    Workspace {
        name: name.to_string(),
    }
}

struct CatalogResolutionPause {
    entered: tokio::sync::oneshot::Sender<()>,
    released: Mutex<mpsc::Receiver<()>>,
}

struct PausingCatalogDecorator {
    pause: Option<CatalogResolutionPause>,
}

impl SourceDecorator for PausingCatalogDecorator {
    fn name(&self) -> &'static str {
        "pausing_catalog_resolution"
    }

    fn supports_provider_discovered_catalogs(&self) -> bool {
        true
    }

    fn prepare(&mut self, _selected_sources: &[QuerySource]) -> Result<(), SourceDecoratorError> {
        let Some(pause) = self.pause.take() else {
            return Ok(());
        };
        pause.entered.send(()).map_err(|()| {
            SourceDecoratorError::failed_precondition("catalog pause receiver closed")
        })?;
        pause
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv()
            .map_err(|_receive_error| {
                SourceDecoratorError::failed_precondition("catalog pause sender closed")
            })
    }

    fn decorate_source(
        &mut self,
        _source: &QuerySource,
        tables: SourceTables,
    ) -> Result<SourceTables, SourceDecoratorError> {
        Ok(tables)
    }
}

#[derive(Default)]
struct PausingCatalogExtensionsProvider {
    next_pause: Mutex<Option<CatalogResolutionPause>>,
}

impl PausingCatalogExtensionsProvider {
    fn arm(&self) -> (tokio::sync::oneshot::Receiver<()>, mpsc::Sender<()>) {
        let (entered_sender, entered_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let previous = self
            .next_pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(CatalogResolutionPause {
                entered: entered_sender,
                released: Mutex::new(release_receiver),
            });
        assert!(previous.is_none(), "catalog resolution pause already armed");
        (entered_receiver, release_sender)
    }
}

impl EngineExtensionsProvider for PausingCatalogExtensionsProvider {
    fn extensions_for(&self, _selected_sources: &[QuerySource]) -> EngineExtensions {
        let mut extensions = EngineExtensions::default();
        let pause = self
            .next_pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if pause.is_some() {
            extensions
                .source_decorators
                .push(Box::new(PausingCatalogDecorator { pause }));
        }
        extensions
    }
}

fn review_queue_function_sql() -> String {
    r"/*
name: get_review_queue
schema: github
description: Get the current viewer's review queue.
guide: Use this function for review queue lookups.
*/

select
  cast($viewer as VARCHAR) as reviewer,
  cast('Review catalog unification' as VARCHAR) as default_queue
"
    .to_string()
}

async fn list_catalog_item(
    harness: &GrpcHarness,
    workspace: &Workspace,
    schema_name: &str,
    item_name: &str,
    kind: CatalogItemKind,
) -> Option<CatalogItem> {
    harness
        .catalog_client()
        .list_catalog(Request::new(ListCatalogRequest {
            workspace: Some(workspace.clone()),
            catalog_name: String::new(),
            schema_name: schema_name.to_string(),
            kind: kind as i32,
            pagination: Some(PaginationRequest {
                limit: 50,
                offset: 0,
            }),
        }))
        .await
        .expect("list catalog")
        .into_inner()
        .items
        .into_iter()
        .find(|item| catalog_item_matches(item, schema_name, item_name))
}

async fn search_entry(
    harness: &GrpcHarness,
    workspace: &Workspace,
    schema_name: &str,
    item_name: &str,
) -> Option<SearchSurfaceRef> {
    search_results(harness, workspace, &format!("{schema_name}.{item_name}"))
        .await
        .into_iter()
        .filter_map(|result| result.surface)
        .find(|entry| entry.schema_name == schema_name && entry.name == item_name)
}

async fn search_results(
    harness: &GrpcHarness,
    workspace: &Workspace,
    query: &str,
) -> Vec<ProtoSearchResult> {
    harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(workspace.clone()),
            query: query.to_string(),
            limit: 50,
        }))
        .await
        .expect("search catalog")
        .into_inner()
        .results
}

fn entry_references(results: &[ProtoSearchResult]) -> Vec<String> {
    results
        .iter()
        .filter_map(|result| result.surface.as_ref())
        .map(|entry| format!("{}.{}", entry.schema_name, entry.name))
        .collect()
}

fn field_names(result: &ProtoSearchResult) -> Vec<&str> {
    match result.shape.as_ref() {
        Some(search_result::Shape::Table(table)) => table
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect(),
        Some(search_result::Shape::Function(function)) => function
            .arguments
            .iter()
            .chain(function.returns.iter())
            .map(|field| field.name.as_str())
            .collect(),
        None => Vec::new(),
    }
}

fn catalog_item_matches(item: &CatalogItem, schema_name: &str, item_name: &str) -> bool {
    match item.item.as_ref() {
        Some(catalog_item::Item::Table(table)) => {
            table.schema_name == schema_name && table.name == item_name
        }
        Some(catalog_item::Item::TableFunction(function)) => {
            function.schema_name == schema_name && function.name == item_name
        }
        None => false,
    }
}

#[tokio::test]
async fn search_and_list_catalog_share_runtime_catalog_items() {
    let harness = GrpcHarness::new().await;
    let workspace = default_workspace();
    harness
        .import_source(table_preview_manifest_yaml(), Vec::new(), Vec::new())
        .await;
    harness
        .import_source(searchable_manifest_yaml(), Vec::new(), Vec::new())
        .await;

    for (schema_name, item_name, kind) in [
        ("coral", "tables", CatalogItemKind::Table),
        ("table_preview", "messages", CatalogItemKind::Table),
        (
            "searchable",
            "search_messages",
            CatalogItemKind::TableFunction,
        ),
    ] {
        let listed = list_catalog_item(&harness, &workspace, schema_name, item_name, kind)
            .await
            .expect("listed catalog item");
        let searched = search_entry(&harness, &workspace, schema_name, item_name)
            .await
            .expect("searched catalog entry");
        // Search returns an identity the caller can query; list_catalog owns
        // the full metadata for it.
        assert!(
            catalog_item_matches(&listed, &searched.schema_name, &searched.name),
            "catalog item {schema_name}.{item_name}"
        );
    }
}

#[tokio::test]
async fn search_and_list_catalog_share_installed_udf_metadata() {
    let harness = GrpcHarness::new().await;
    let workspace = default_workspace();

    search_entry(&harness, &workspace, "coral", "tables")
        .await
        .expect("prime catalog search projection");

    harness
        .function_client()
        .add_function(Request::new(AddFunctionRequest {
            workspace: Some(workspace.clone()),
            sql: review_queue_function_sql(),
            fail_if_exists: false,
            write_surface: 0,
        }))
        .await
        .expect("add review queue function");

    let rebuilt = harness
        .search_client()
        .rebuild_search_index(Request::new(RebuildSearchIndexRequest {
            workspace: Some(workspace.clone()),
            provider: SearchIndexProvider::Catalog as i32,
            force: false,
        }))
        .await
        .expect("rebuild catalog with installed function")
        .into_inner();
    let rebuild_detail = catalog_rebuild_detail(&rebuilt);
    assert!(rebuild_detail.rebuild_performed);
    assert!(rebuild_detail.projection_changed);

    let listed = list_catalog_item(
        &harness,
        &workspace,
        "github",
        "get_review_queue",
        CatalogItemKind::TableFunction,
    )
    .await
    .expect("listed review queue function");
    let searched = search_entry(&harness, &workspace, "github", "get_review_queue")
        .await
        .expect("searched review queue function");

    assert_eq!(searched.schema_name, "github");
    assert_eq!(searched.name, "get_review_queue");
    // Full function metadata stays with list_catalog; search returns identity.
    match listed.item {
        Some(catalog_item::Item::TableFunction(function)) => {
            assert_eq!(
                function.guide,
                "Use this function for review queue lookups."
            );
            assert_eq!(function.arguments[0].name, "viewer");
            assert_eq!(function.result_columns[0].name, "reviewer");
        }
        _ => panic!("expected a table function"),
    }
}

#[tokio::test]
async fn natural_language_review_queue_query_ranks_installed_udf_in_top_three() {
    let harness = GrpcHarness::new().await;
    harness
        .function_client()
        .add_function(Request::new(AddFunctionRequest {
            workspace: Some(default_workspace()),
            sql: review_queue_function_sql(),
            fail_if_exists: false,
            write_surface: 0,
        }))
        .await
        .expect("add review queue function");

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "GitHub review queue table function".to_string(),
            limit: 0,
        }))
        .await
        .expect("search review queue")
        .into_inner();
    let position = response
        .results
        .iter()
        .position(|result| {
            result.surface.as_ref().is_some_and(|entry| {
                entry.schema_name == "github" && entry.name == "get_review_queue"
            })
        })
        .expect("review queue function in default search window");

    assert!(position < 3, "review queue function ranked at {position}");
}

#[tokio::test]
async fn unified_catalog_keeps_source_metadata_isolated_by_workspace() {
    let harness = GrpcHarness::new().await;
    let default = default_workspace();
    let work = workspace("work");
    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(work.clone()),
        }))
        .await
        .expect("create work workspace");

    harness
        .import_source(table_preview_manifest_yaml(), Vec::new(), Vec::new())
        .await;

    let default_listed = list_catalog_item(
        &harness,
        &default,
        "table_preview",
        "messages",
        CatalogItemKind::Table,
    )
    .await
    .expect("listed default workspace table");
    let default_searched = search_entry(&harness, &default, "table_preview", "messages")
        .await
        .expect("searched default workspace table");

    assert!(catalog_item_matches(
        &default_listed,
        &default_searched.schema_name,
        &default_searched.name
    ));
    assert!(
        list_catalog_item(
            &harness,
            &work,
            "table_preview",
            "messages",
            CatalogItemKind::Table,
        )
        .await
        .is_none()
    );
    assert!(
        search_entry(&harness, &work, "table_preview", "messages")
            .await
            .is_none()
    );

    let work_system_table =
        list_catalog_item(&harness, &work, "coral", "tables", CatalogItemKind::Table)
            .await
            .expect("listed work system table");
    let searched_work_system_table = search_entry(&harness, &work, "coral", "tables")
        .await
        .expect("searched work system table");
    assert!(catalog_item_matches(
        &work_system_table,
        &searched_work_system_table.schema_name,
        &searched_work_system_table.name
    ));
    match work_system_table.item {
        Some(catalog_item::Item::Table(table)) => {
            assert_eq!(table.workspace.as_ref(), Some(&work));
        }
        Some(catalog_item::Item::TableFunction(_)) | None => panic!("expected table"),
    }
}

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
    let truncation = response.truncation.as_ref().expect("truncation");
    assert!(!truncation.truncated);
    assert_eq!(truncation.returned_count, 0);
    assert_eq!(truncation.max_results, 10);
    assert_eq!(response.provider_statuses.len(), 3);
    let catalog_status = assert_provider_state(
        &response,
        SearchProvider::CatalogMetadata,
        SearchProviderState::Empty,
    );
    assert_empty_provider_coverage(catalog_status);
    let observed_status = assert_provider_state(
        &response,
        SearchProvider::ObservedValues,
        SearchProviderState::NotEnabled,
    );
    assert_no_coverage(observed_status);
    assert!(observed_status.note.contains("`observed_values_search`"));
    let native_status = assert_provider_state(
        &response,
        SearchProvider::NativeFanout,
        SearchProviderState::NotEnabled,
    );
    assert_no_coverage(native_status);
}

#[test]
fn search_completes_with_one_blocking_thread() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .expect("build constrained Tokio runtime");
    let search = runtime.block_on(async {
        let harness = GrpcHarness::new().await;
        let search = tokio::time::timeout(
            Duration::from_secs(5),
            harness.search_client().search(Request::new(SearchRequest {
                workspace: Some(default_workspace()),
                query: "github issue".to_string(),
                limit: 10,
            })),
        )
        .await;
        drop(harness);
        search
    });
    // A failed regression must not leave the test process waiting forever for
    // a blocked task while Tokio tears down its constrained blocking pool.
    runtime.shutdown_timeout(Duration::from_secs(1));

    let response = search
        .expect("search should not starve the blocking pool")
        .expect("search")
        .into_inner();
    assert_eq!(response.provider_statuses.len(), 3);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_paused_in_catalog_resolution_does_not_recreate_deleted_workspace_storage() {
    let extensions = Arc::new(PausingCatalogExtensionsProvider::default());
    let provider: Arc<dyn EngineExtensionsProvider> = extensions.clone();
    let harness = GrpcHarness::new_with_engine_extensions_provider(provider).await;
    let work = workspace("work");
    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(work.clone()),
        }))
        .await
        .expect("create work workspace");

    let (catalog_entered, release_catalog) = extensions.arm();
    let mut search_client = harness.search_client();
    let search_workspace = work.clone();
    let search_task = tokio::spawn(async move {
        search_client
            .search(Request::new(SearchRequest {
                workspace: Some(search_workspace),
                query: "coral tables".to_string(),
                limit: 10,
            }))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), catalog_entered)
        .await
        .expect("search should enter catalog runtime preparation")
        .expect("catalog runtime preparation should signal the test");

    let deletion = tokio::time::timeout(
        Duration::from_secs(5),
        harness
            .workspace_client()
            .delete_workspace(Request::new(DeleteWorkspaceRequest {
                workspace: Some(work.clone()),
            })),
    )
    .await;
    release_catalog
        .send(())
        .expect("catalog runtime preparation should remain paused");
    deletion
        .expect("workspace deletion should not wait for catalog preparation")
        .expect("delete work workspace");

    let search_status = tokio::time::timeout(Duration::from_secs(5), search_task)
        .await
        .expect("search should finish after catalog preparation resumes")
        .expect("search task should not panic")
        .expect_err("search for a deleted workspace should fail");
    assert_eq!(search_status.code(), Code::NotFound);

    let workspace_dir = harness.config_dir().join("workspaces/work");
    assert!(
        !workspace_dir.exists(),
        "search must not recreate storage for a deleted workspace"
    );
    assert!(
        !workspace_dir.join("search/search.sqlite3").exists(),
        "search must not recreate a deleted workspace search projection"
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
    harness
        .import_source(searchable_manifest_yaml(), Vec::new(), Vec::new())
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
    assert_eq!(
        function.guide,
        "Prefer this provider route for message lookup."
    );
    let search_limits = function.search_limits.as_ref().expect("search limits");
    assert_eq!(search_limits.default_top_k, 5);
    assert_eq!(search_limits.max_top_k, 20);
    assert_eq!(search_limits.max_calls_per_query, 2);
}

#[tokio::test]
async fn search_matches_table_function_guide() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(searchable_manifest_yaml(), Vec::new(), Vec::new())
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "provider route".to_string(),
            limit: 10,
        }))
        .await
        .expect("search table function guide")
        .into_inner();

    assert!(
        entry_references(&response.results).contains(&"searchable.search_messages".to_string()),
        "guide match should return the function as a queryable entry"
    );
}

#[tokio::test]
async fn search_returns_catalog_metadata_for_search_functions_and_column_hints() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(searchable_manifest_yaml(), Vec::new(), Vec::new())
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "messages title".to_string(),
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
    let observed_status = assert_provider_state(
        &response,
        SearchProvider::ObservedValues,
        SearchProviderState::NotEnabled,
    );
    assert_no_coverage(observed_status);
    assert_provider_state(
        &response,
        SearchProvider::NativeFanout,
        SearchProviderState::NotEnabled,
    );
    assert!(
        response
            .truncation
            .as_ref()
            .is_some_and(|truncation| !truncation.truncated)
    );
    assert!(
        harness
            .config_dir()
            .join("workspaces")
            .join("default")
            .join("search")
            .join("search.sqlite3")
            .exists(),
        "search should create the workspace SQLite search database"
    );
    let function_result = response
        .results
        .iter()
        .find(|result| {
            result
                .surface
                .as_ref()
                .is_some_and(|entry| entry.name == "search_messages")
        })
        .expect("search function returned as a queryable entry");
    // The matching result column arrives as a field on the function that owns
    // it rather than as a peer result.
    assert!(
        field_names(function_result).contains(&"title"),
        "matched result column should nest under its function: {function_result:?}"
    );
}

#[tokio::test]
async fn search_reports_partial_catalog_coverage_and_recovers_after_source_repair() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(table_preview_manifest_yaml(), Vec::new(), Vec::new())
        .await;
    let repair_manifest = searchable_manifest_yaml();
    harness
        .import_source(repair_manifest.clone(), Vec::new(), Vec::new())
        .await;
    let broken_manifest = harness
        .config_dir()
        .join("workspaces/default/sources/searchable/manifest.yaml");
    fs::write(&broken_manifest, "name: [invalid").expect("break installed manifest");

    let degraded = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "table_preview.messages".to_string(),
            limit: 10,
        }))
        .await
        .expect("search healthy source")
        .into_inner();
    assert!(degraded.results.iter().any(|result| {
        result
            .surface
            .as_ref()
            .is_some_and(|entry| entry.schema_name == "table_preview" && entry.name == "messages")
    }));
    let status = assert_provider_state(
        &degraded,
        SearchProvider::CatalogMetadata,
        SearchProviderState::Partial,
    );
    let coverage = status.coverage.as_ref().expect("catalog coverage");
    assert_eq!(coverage.failed_units, 1);
    assert!(coverage.stale_index);
    assert!(!coverage.has_more);
    assert!(status.note.contains("searchable"));

    fs::write(&broken_manifest, repair_manifest).expect("repair installed manifest");
    let recovered = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "searchable.search_messages".to_string(),
            limit: 10,
        }))
        .await
        .expect("search repaired catalog")
        .into_inner();
    assert!(recovered.results.iter().any(|result| {
        result.surface.as_ref().is_some_and(|entry| {
            entry.schema_name == "searchable" && entry.name == "search_messages"
        })
    }));
    let status = assert_provider_state(
        &recovered,
        SearchProvider::CatalogMetadata,
        SearchProviderState::ResultsFound,
    );
    let coverage = status.coverage.as_ref().expect("catalog coverage");
    assert_eq!(coverage.failed_units, 0);
    assert!(!coverage.stale_index);
}

#[tokio::test]
async fn search_isolates_identity_gated_source_as_partial_catalog_failure() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(table_preview_manifest_yaml(), Vec::new(), Vec::new())
        .await;

    let descriptor_temp = tempfile::tempdir().expect("descriptor temp dir");
    let openapi_file = descriptor_temp.path().join("identity-guard-openapi.yaml");
    fs::write(
        &openapi_file,
        r"
openapi: 3.0.3
info: {title: Identity Guard}
paths:
  /items:
    get:
      operationId: items/list
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    id: {type: integer}
",
    )
    .expect("write OpenAPI fixture");
    harness
        .import_source(
            format!(
                r"
name: github_v4_identity_guard
dsl_version: 4
identity_requirements:
  accepts:
    - id: github_api
      identity_specs: [github_oauth]
surface:
  type: openapi
  file: {}
",
                openapi_file.display()
            ),
            Vec::new(),
            Vec::new(),
        )
        .await;
    fs::remove_file(&openapi_file).expect("remove authored descriptor after import");

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "table_preview.messages".to_string(),
            limit: 10,
        }))
        .await
        .expect("search healthy source")
        .into_inner();

    assert!(response.results.iter().any(|result| {
        result
            .surface
            .as_ref()
            .is_some_and(|entry| entry.schema_name == "table_preview" && entry.name == "messages")
    }));
    let status = assert_provider_state(
        &response,
        SearchProvider::CatalogMetadata,
        SearchProviderState::Partial,
    );
    let coverage = status.coverage.as_ref().expect("catalog coverage");
    assert_eq!(coverage.failed_units, 1);
    assert!(coverage.stale_index);
    assert!(!coverage.has_more);
    assert!(status.note.contains("github_v4_identity_guard"));
}

#[tokio::test]
async fn rebuild_search_index_forces_catalog_projection_refresh() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(searchable_manifest_yaml(), Vec::new(), Vec::new())
        .await;

    let first = harness
        .search_client()
        .rebuild_search_index(Request::new(RebuildSearchIndexRequest {
            workspace: Some(default_workspace()),
            provider: SearchIndexProvider::Catalog as i32,
            force: false,
        }))
        .await
        .expect("rebuild catalog")
        .into_inner();
    let first_detail = catalog_rebuild_detail(&first);
    assert_eq!(first_detail.old_document_count, 0);
    assert!(first_detail.new_document_count > 0);
    assert!(first_detail.projection_changed);
    assert!(first_detail.rebuild_performed);
    assert_eq!(
        SearchMaintenanceState::try_from(catalog_rebuild_result(&first).state)
            .expect("maintenance state"),
        SearchMaintenanceState::Completed
    );

    let forced = harness
        .search_client()
        .rebuild_search_index(Request::new(RebuildSearchIndexRequest {
            workspace: Some(default_workspace()),
            provider: SearchIndexProvider::Catalog as i32,
            force: true,
        }))
        .await
        .expect("force rebuild catalog")
        .into_inner();
    let forced_detail = catalog_rebuild_detail(&forced);
    assert_eq!(
        forced_detail.old_document_count,
        first_detail.new_document_count
    );
    assert_eq!(
        forced_detail.new_document_count,
        first_detail.new_document_count
    );
    assert!(!forced_detail.projection_changed);
    assert!(
        forced_detail.rebuild_performed,
        "force should rebuild even when the fingerprint is current"
    );
    assert_eq!(
        SearchMaintenanceState::try_from(catalog_rebuild_result(&forced).state)
            .expect("maintenance state"),
        SearchMaintenanceState::Completed
    );
}

#[tokio::test]
async fn rebuild_search_index_unspecified_rebuilds_catalog_and_observed_values() {
    let harness = GrpcHarness::new_with_observed_values_search().await;
    harness
        .import_source(searchable_manifest_yaml(), Vec::new(), Vec::new())
        .await;

    let response = harness
        .search_client()
        .rebuild_search_index(Request::new(RebuildSearchIndexRequest {
            workspace: Some(default_workspace()),
            provider: SearchIndexProvider::Unspecified as i32,
            force: false,
        }))
        .await
        .expect("rebuild all search indexes")
        .into_inner();

    assert_eq!(response.results.len(), 2);
    let catalog_detail = catalog_rebuild_detail(&response);
    assert!(catalog_detail.new_document_count > 0);

    let observed = rebuild_result(&response, SearchProvider::ObservedValues);
    assert_eq!(
        SearchMaintenanceState::try_from(observed.state).expect("observed maintenance state"),
        SearchMaintenanceState::Completed
    );

    let observed_rebuild = observed_rebuild_detail(&response);
    assert_eq!(observed_rebuild.canonical_rows_scanned, 0);
    assert_eq!(observed_rebuild.fts_rows_rebuilt, 0);
}

#[tokio::test]
async fn rebuild_search_index_all_rebuilds_catalog_and_skips_disabled_observed_values() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(searchable_manifest_yaml(), Vec::new(), Vec::new())
        .await;

    let response = harness
        .search_client()
        .rebuild_search_index(Request::new(RebuildSearchIndexRequest {
            workspace: Some(default_workspace()),
            provider: SearchIndexProvider::All as i32,
            force: false,
        }))
        .await
        .expect("rebuild all search indexes")
        .into_inner();

    assert_eq!(response.results.len(), 2);
    assert!(catalog_rebuild_detail(&response).new_document_count > 0);
    assert_disabled_observed_maintenance_result(rebuild_result(
        &response,
        SearchProvider::ObservedValues,
    ));
}

#[tokio::test]
async fn rebuild_search_index_observed_values_skips_without_creating_storage_when_disabled() {
    let harness = GrpcHarness::new().await;
    let sqlite_path = search_sqlite_path(&harness);
    assert!(!sqlite_path.exists());

    let response = harness
        .search_client()
        .rebuild_search_index(Request::new(RebuildSearchIndexRequest {
            workspace: Some(default_workspace()),
            provider: SearchIndexProvider::ObservedValues as i32,
            force: false,
        }))
        .await
        .expect("disabled observed-value search rebuild")
        .into_inner();

    assert_eq!(response.results.len(), 1);
    assert_disabled_observed_maintenance_result(rebuild_result(
        &response,
        SearchProvider::ObservedValues,
    ));
    assert!(
        !sqlite_path.exists(),
        "disabled observed rebuild should not create search storage"
    );
}

#[tokio::test]
async fn rebuild_search_index_observed_values_rebuilds_projection() {
    let harness = GrpcHarness::new_with_observed_values_search().await;

    let response = harness
        .search_client()
        .rebuild_search_index(Request::new(RebuildSearchIndexRequest {
            workspace: Some(default_workspace()),
            provider: SearchIndexProvider::ObservedValues as i32,
            force: false,
        }))
        .await
        .expect("rebuild observed-value search index")
        .into_inner();

    assert_eq!(response.results.len(), 1);
    let observed = rebuild_result(&response, SearchProvider::ObservedValues);
    assert_eq!(
        SearchMaintenanceState::try_from(observed.state).expect("observed maintenance state"),
        SearchMaintenanceState::Completed
    );
    let detail = observed_rebuild_detail(&response);
    assert_eq!(detail.canonical_rows_scanned, 0);
    assert_eq!(detail.fts_rows_rebuilt, 0);
    let drain = detail.drain.as_ref().expect("pre-rebuild drain detail");
    assert_eq!(drain.queue_jobs_processed, 0);
    assert_eq!(drain.remaining_queue_depth, 0);
    assert_eq!(drain.storage_jobs_dropped, 0);
}

#[tokio::test]
async fn drain_search_queue_rejects_invalid_budget_when_disabled() {
    let harness = GrpcHarness::new().await;
    let sqlite_path = search_sqlite_path(&harness);
    assert!(!sqlite_path.exists());

    let status = harness
        .search_client()
        .drain_search_queue(Request::new(DrainSearchQueueRequest {
            workspace: Some(default_workspace()),
            budget_ms: 60_001,
        }))
        .await
        .expect_err("oversized drain budget should fail");

    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        !sqlite_path.exists(),
        "invalid disabled drain should not create search storage"
    );
}

#[tokio::test]
async fn drain_search_queue_skips_without_creating_storage_when_disabled() {
    let harness = GrpcHarness::new().await;
    let sqlite_path = search_sqlite_path(&harness);
    assert!(!sqlite_path.exists());

    let response = harness
        .search_client()
        .drain_search_queue(Request::new(DrainSearchQueueRequest {
            workspace: Some(default_workspace()),
            budget_ms: 1_000,
        }))
        .await
        .expect("disabled observed-value queue drain")
        .into_inner();

    assert_eq!(response.results.len(), 1);
    assert_disabled_observed_maintenance_result(&response.results[0]);
    assert!(
        !sqlite_path.exists(),
        "disabled observed drain should not create search storage"
    );
}

#[tokio::test]
async fn drain_search_queue_reports_observed_provider_detail() {
    let harness = GrpcHarness::new_with_observed_values_search().await;

    let response = harness
        .search_client()
        .drain_search_queue(Request::new(DrainSearchQueueRequest {
            workspace: Some(default_workspace()),
            budget_ms: 1_000,
        }))
        .await
        .expect("drain observed-value search queue")
        .into_inner();

    assert_eq!(response.results.len(), 1);
    let observed = response
        .results
        .iter()
        .find(|result| result.provider == SearchProvider::ObservedValues as i32)
        .expect("observed provider result");
    assert_eq!(
        SearchMaintenanceState::try_from(observed.state).expect("observed maintenance state"),
        SearchMaintenanceState::Noop
    );
    match observed.detail.as_ref() {
        Some(search_maintenance_result::Detail::ObservedDrain(detail)) => {
            assert_eq!(detail.queue_jobs_processed, 0);
            assert_eq!(detail.remaining_queue_depth, 0);
            assert!(!detail.budget_exhausted);
        }
        other => panic!("expected observed drain detail, got {other:?}"),
    }
}

#[tokio::test]
async fn clear_search_data_remains_available_when_observed_values_search_is_disabled() {
    let harness = GrpcHarness::new().await;

    let response = harness
        .search_client()
        .clear_search_data(Request::new(ClearSearchDataRequest {
            workspace: Some(default_workspace()),
            scope: SearchDataScope::ObservedValues as i32,
            target: Some(SearchClearTarget {
                target: Some(search_clear_target::Target::Workspace(true)),
            }),
        }))
        .await
        .expect("clear disabled observed-value search data")
        .into_inner();

    assert_eq!(response.results.len(), 1);
    assert_eq!(
        response.results[0].provider,
        SearchProvider::ObservedValues as i32
    );
    assert!(matches!(
        response.results[0].detail.as_ref(),
        Some(search_maintenance_result::Detail::ObservedClear(_))
    ));
    assert!(response.storage_cleanup.is_some());
}

#[tokio::test]
async fn clear_search_data_accepts_source_target_with_internal_whitespace() {
    let harness = GrpcHarness::new().await;

    let response = harness
        .search_client()
        .clear_search_data(Request::new(ClearSearchDataRequest {
            workspace: Some(default_workspace()),
            scope: SearchDataScope::ObservedValues as i32,
            target: Some(SearchClearTarget {
                target: Some(search_clear_target::Target::SourceName(
                    "github issues".to_string(),
                )),
            }),
        }))
        .await
        .expect("valid source target with internal whitespace")
        .into_inner();

    assert_eq!(response.results.len(), 1);
}

#[tokio::test]
async fn clear_search_data_rejects_invalid_source_targets_at_transport_edge() {
    let harness = GrpcHarness::new().await;

    for source_name in [
        "",
        " github",
        "github ",
        "github/child",
        r"github\child",
        ".",
        "..",
    ] {
        let status = harness
            .search_client()
            .clear_search_data(Request::new(ClearSearchDataRequest {
                workspace: Some(default_workspace()),
                scope: SearchDataScope::ObservedValues as i32,
                target: Some(SearchClearTarget {
                    target: Some(search_clear_target::Target::SourceName(
                        source_name.to_string(),
                    )),
                }),
            }))
            .await
            .expect_err("invalid source target should fail");

        assert_eq!(
            status.code(),
            Code::InvalidArgument,
            "source={source_name:?}, message={}",
            status.message()
        );
    }
}

#[tokio::test]
async fn clear_search_data_removes_catalog_projection_and_next_search_recreates_it() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(searchable_manifest_yaml(), Vec::new(), Vec::new())
        .await;

    harness
        .search_client()
        .rebuild_search_index(Request::new(RebuildSearchIndexRequest {
            workspace: Some(default_workspace()),
            provider: SearchIndexProvider::Catalog as i32,
            force: false,
        }))
        .await
        .expect("seed catalog projection");
    let clear = harness
        .search_client()
        .clear_search_data(Request::new(ClearSearchDataRequest {
            workspace: Some(default_workspace()),
            scope: SearchDataScope::All as i32,
            target: Some(SearchClearTarget {
                target: Some(search_clear_target::Target::Workspace(true)),
            }),
        }))
        .await
        .expect("clear search data")
        .into_inner();

    let clear_detail = catalog_clear_detail(&clear);
    assert!(clear_detail.deleted_document_count > 0);
    let cleanup = clear
        .storage_cleanup
        .as_ref()
        .expect("storage cleanup result");
    assert_eq!(
        SearchMaintenanceState::try_from(cleanup.state).expect("cleanup state"),
        SearchMaintenanceState::Completed
    );
    assert!(!cleanup.note.contains("SQLite"));
    assert!(!cleanup.note.contains("WAL"));
    assert!(!cleanup.note.contains("VACUUM"));

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "messages title".to_string(),
            limit: 10,
        }))
        .await
        .expect("search after clear")
        .into_inner();

    assert_provider_state(
        &response,
        SearchProvider::CatalogMetadata,
        SearchProviderState::ResultsFound,
    );
    assert!(
        response
            .results
            .iter()
            .any(|result| result.surface.is_some()),
        "next search should recreate and use the catalog projection"
    );
}

#[tokio::test]
async fn source_scoped_all_clear_does_not_load_manifest_and_bumps_generation() {
    let harness = GrpcHarness::new_with_observed_values_search().await;
    harness
        .import_source(searchable_manifest_yaml(), Vec::new(), Vec::new())
        .await;
    harness
        .search_client()
        .rebuild_search_index(Request::new(RebuildSearchIndexRequest {
            workspace: Some(default_workspace()),
            provider: SearchIndexProvider::Catalog as i32,
            force: false,
        }))
        .await
        .expect("seed catalog projection");
    let sqlite_path = harness
        .config_dir()
        .join("workspaces/default/search/search.sqlite3");
    let initial_generation: i64 = rusqlite::Connection::open(&sqlite_path)
        .expect("open search sqlite before clear")
        .query_row(
            "SELECT generation FROM observed_source_generations WHERE workspace = 'default' AND source_name = 'searchable'",
            [],
            |row| row.get(0),
        )
        .expect("initial source generation");
    std::fs::remove_file(source_dir(harness.config_dir(), "searchable").join("manifest.yaml"))
        .expect("remove manifest before clear");

    let clear = harness
        .search_client()
        .clear_search_data(Request::new(ClearSearchDataRequest {
            workspace: Some(default_workspace()),
            scope: SearchDataScope::All as i32,
            target: Some(SearchClearTarget {
                target: Some(search_clear_target::Target::SourceName(
                    "searchable".to_string(),
                )),
            }),
        }))
        .await
        .expect("source-scoped all clear")
        .into_inner();

    assert_eq!(clear.results.len(), 2);
    assert!(catalog_clear_detail(&clear).deleted_document_count > 0);
    assert!(clear.results.iter().any(|result| matches!(
        result.detail.as_ref(),
        Some(search_maintenance_result::Detail::ObservedClear(_))
    )));
    let connection = rusqlite::Connection::open(sqlite_path).expect("open search sqlite");
    let generation: i64 = connection
        .query_row(
            "SELECT generation FROM observed_source_generations WHERE workspace = 'default' AND source_name = 'searchable'",
            [],
            |row| row.get(0),
        )
        .expect("source generation");
    assert_eq!(generation, initial_generation + 1);
}

#[tokio::test]
async fn search_table_preview_columns_do_not_inherit_table_matched_fields() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(table_preview_manifest_yaml(), Vec::new(), Vec::new())
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "conversation archive".to_string(),
            limit: 10,
        }))
        .await
        .expect("search")
        .into_inner();

    let result = response
        .results
        .iter()
        .find(|result| {
            result
                .surface
                .as_ref()
                .is_some_and(|entry| entry.name == "messages")
        })
        .expect("messages table entry");
    // The table matched on its description, not on any column, so no column is
    // reported as matching evidence.
    assert!(
        field_names(result).is_empty(),
        "a description match must not present columns as matched evidence: {result:?}"
    );
    assert_eq!(
        result.omitted_matching_field_count, 0,
        "no matching fields were omitted because none matched"
    );
}

#[tokio::test]
async fn search_groups_observed_values_under_their_table() {
    let harness = GrpcHarness::new_with_observed_values_search().await;
    let source = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{ "text": "world" }]
        })))
        .mount(&source)
        .await;
    harness
        .import_source(
            observed_values_manifest_yaml(&source.uri()),
            Vec::new(),
            Vec::new(),
        )
        .await;
    harness
        .execute_sql_rows("SELECT text FROM observed_fixture.messages")
        .await;

    let sqlite_path = harness
        .config_dir()
        .join("workspaces/default/search/search.sqlite3");
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let queued = rusqlite::Connection::open(&sqlite_path)
                .ok()
                .and_then(|connection| {
                    connection
                        .query_row("SELECT COUNT(*) FROM observed_queue_jobs", [], |row| {
                            row.get::<_, i64>(0)
                        })
                        .ok()
                })
                .unwrap_or_default();
            if queued > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("observed-value job should reach the durable queue");

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "world".to_string(),
            limit: 10,
        }))
        .await
        .expect("search observed values")
        .into_inner();
    let result = response
        .results
        .iter()
        .find(|result| {
            result.surface.as_ref().is_some_and(|surface| {
                surface.schema_name == "observed_fixture" && surface.name == "messages"
            })
        })
        .expect("observed value should elect its table");

    assert!(
        result
            .providers
            .contains(&(SearchProvider::ObservedValues as i32))
    );
    assert!(result.matching_values.iter().any(|values| {
        values.field == "text" && values.values.iter().any(|value| value == "world")
    }));
}

#[tokio::test]
async fn search_provider_coverage_counts_mapped_candidates() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            many_matching_columns_manifest_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "alpha".to_string(),
            limit: 10,
        }))
        .await
        .expect("search")
        .into_inner();

    // Five columns match, but they belong to one table, so the response holds
    // one queryable entry rather than one result per matching column.
    assert_eq!(response.results.len(), 1);
    let result = response.results.first().expect("records entry");
    assert_eq!(
        result.surface.as_ref().map(|entry| entry.name.as_str()),
        Some("records")
    );

    let mut matched = field_names(result);
    matched.sort_unstable();
    assert_eq!(
        matched,
        [
            "alpha_five",
            "alpha_four",
            "alpha_one",
            "alpha_three",
            "alpha_two"
        ],
        "every matching column should nest under the table that owns it"
    );
    assert_eq!(
        result.omitted_matching_field_count, 0,
        "five matching fields fit inside the per-entry cap"
    );
}

#[tokio::test]
async fn search_truncation_reflects_provider_retrieval_limit() {
    let harness = GrpcHarness::new().await;
    harness
        .import_source(
            many_retrieved_columns_manifest_yaml(),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "alpha".to_string(),
            limit: 10,
        }))
        .await
        .expect("search")
        .into_inner();

    let catalog_status = assert_provider_state(
        &response,
        SearchProvider::CatalogMetadata,
        SearchProviderState::Partial,
    );
    let coverage = catalog_status.coverage.as_ref().expect("coverage");
    assert!(
        coverage.has_more,
        "catalog provider should report that SQLite retrieval was capped"
    );
    let truncation = response.truncation.as_ref().expect("truncation");
    assert!(
        truncation.truncated,
        "top-level truncation should include provider retrieval caps"
    );
}

#[tokio::test]
async fn observed_search_handles_live_scopes_beyond_sqlite_variable_limit() {
    let harness = GrpcHarness::new_with_observed_values_search().await;
    harness
        .import_source(
            many_table_surfaces_manifest_yaml(SQLITE_VARIABLE_LIMIT_REGRESSION_SURFACE_COUNT),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let response = harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: "nonexistent".to_string(),
            limit: 10,
        }))
        .await
        .expect("search")
        .into_inner();

    let status = assert_provider_state(
        &response,
        SearchProvider::ObservedValues,
        SearchProviderState::Empty,
    );
    assert_empty_provider_coverage(status);
    assert!(
        !status.note.contains("too many SQL variables"),
        "unexpected observed-values provider note: {}",
        status.note
    );
}

fn assert_provider_state(
    response: &coral_api::v1::SearchResponse,
    provider: SearchProvider,
    state: SearchProviderState,
) -> &coral_api::v1::SearchProviderStatus {
    let status = response
        .provider_statuses
        .iter()
        .find(|status| status.provider == provider as i32)
        .expect("provider status");
    assert_eq!(
        SearchProviderState::try_from(status.state).expect("provider state"),
        state
    );
    status
}

fn assert_no_coverage(status: &coral_api::v1::SearchProviderStatus) {
    assert!(
        status.coverage.is_none(),
        "disabled provider coverage should be absent"
    );
}

fn assert_empty_provider_coverage(status: &coral_api::v1::SearchProviderStatus) {
    let coverage = status.coverage.as_ref().expect("provider coverage");
    assert_eq!(coverage.failed_units, 0);
    assert_eq!(coverage.returned_count, 0);
    assert!(!coverage.has_more);
    assert!(!coverage.budget_exhausted);
    assert!(!coverage.timed_out);
    assert!(!coverage.stale_index);
}

fn catalog_rebuild_detail(
    response: &coral_api::v1::RebuildSearchIndexResponse,
) -> &coral_api::v1::CatalogRebuildResult {
    let result = catalog_rebuild_result(response);
    match result.detail.as_ref() {
        Some(search_maintenance_result::Detail::CatalogRebuild(detail)) => detail,
        other => panic!("expected catalog rebuild detail, got {other:?}"),
    }
}

fn catalog_rebuild_result(
    response: &coral_api::v1::RebuildSearchIndexResponse,
) -> &coral_api::v1::SearchMaintenanceResult {
    response
        .results
        .iter()
        .find(|result| result.provider == SearchProvider::CatalogMetadata as i32)
        .expect("provider result")
}

fn rebuild_result(
    response: &coral_api::v1::RebuildSearchIndexResponse,
    provider: SearchProvider,
) -> &coral_api::v1::SearchMaintenanceResult {
    response
        .results
        .iter()
        .find(|result| result.provider == provider as i32)
        .expect("maintenance result")
}

fn observed_rebuild_detail(
    response: &coral_api::v1::RebuildSearchIndexResponse,
) -> &coral_api::v1::ObservedRebuildResult {
    response
        .results
        .iter()
        .find_map(|result| match result.detail.as_ref()? {
            search_maintenance_result::Detail::ObservedRebuild(detail) => Some(detail),
            _ => None,
        })
        .expect("observed rebuild detail")
}

fn assert_disabled_observed_maintenance_result(result: &coral_api::v1::SearchMaintenanceResult) {
    assert_eq!(result.provider, SearchProvider::ObservedValues as i32);
    assert_eq!(
        SearchMaintenanceState::try_from(result.state).expect("observed maintenance state"),
        SearchMaintenanceState::Skipped
    );
    assert!(
        result.note.contains("enable") && result.note.contains("`observed_values_search`"),
        "disabled observed maintenance note should explain how to enable it: {}",
        result.note
    );
    assert!(
        result.detail.is_none(),
        "disabled observed maintenance should not report provider detail"
    );
}

fn search_sqlite_path(harness: &GrpcHarness) -> std::path::PathBuf {
    harness
        .config_dir()
        .join("workspaces/default/search/search.sqlite3")
}

fn catalog_clear_detail(
    response: &coral_api::v1::ClearSearchDataResponse,
) -> &coral_api::v1::CatalogClearResult {
    response
        .results
        .iter()
        .find_map(|result| match result.detail.as_ref()? {
            search_maintenance_result::Detail::CatalogClear(detail) => Some(detail),
            _ => None,
        })
        .expect("catalog clear detail")
}

fn searchable_manifest_yaml() -> String {
    manifest_yaml(&json!({
        "name": "searchable",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": "https://example.test",
        "functions": [{
            "name": "search_messages",
            "kind": "search",
            "description": "Search messages",
            "guide": "Prefer this provider route for message lookup.",
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
    }))
}

fn table_preview_manifest_yaml() -> String {
    manifest_yaml(&json!({
        "name": "table_preview",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": "https://example.test",
        "tables": [{
            "name": "messages",
            "description": "Conversation archive",
            "request": { "method": "GET", "path": "/messages" },
            "response": {},
            "pagination": { "mode": "none" },
            "columns": [
                {
                    "name": "id",
                    "type": "Utf8",
                    "nullable": false,
                    "description": "Message id",
                    "expr": { "kind": "path", "path": ["id"] }
                },
                {
                    "name": "author",
                    "type": "Utf8",
                    "nullable": true,
                    "description": "Author name",
                    "expr": { "kind": "path", "path": ["author"] }
                }
            ]
        }]
    }))
}

fn observed_values_manifest_yaml(base_url: &str) -> String {
    manifest_yaml(&json!({
        "name": "observed_fixture",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": base_url,
        "tables": [{
            "name": "messages",
            "description": "Observed-value fixture",
            "request": { "method": "GET", "path": "/messages" },
            "response": { "rows_path": ["items"] },
            "pagination": { "mode": "none" },
            "columns": [{
                "name": "text",
                "type": "Utf8",
                "nullable": false,
                "expr": { "kind": "path", "path": ["text"] }
            }]
        }]
    }))
}

fn many_matching_columns_manifest_yaml() -> String {
    manifest_yaml(&json!({
        "name": "many_matching_columns",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": "https://example.test",
        "tables": [{
            "name": "records",
            "description": "Alpha archive",
            "request": { "method": "GET", "path": "/records" },
            "response": {},
            "pagination": { "mode": "none" },
            "columns": [
                {
                    "name": "alpha_one",
                    "type": "Utf8",
                    "nullable": true,
                    "description": "",
                    "expr": { "kind": "path", "path": ["alpha_one"] }
                },
                {
                    "name": "alpha_two",
                    "type": "Utf8",
                    "nullable": true,
                    "description": "",
                    "expr": { "kind": "path", "path": ["alpha_two"] }
                },
                {
                    "name": "alpha_three",
                    "type": "Utf8",
                    "nullable": true,
                    "description": "",
                    "expr": { "kind": "path", "path": ["alpha_three"] }
                },
                {
                    "name": "alpha_four",
                    "type": "Utf8",
                    "nullable": true,
                    "description": "",
                    "expr": { "kind": "path", "path": ["alpha_four"] }
                },
                {
                    "name": "alpha_five",
                    "type": "Utf8",
                    "nullable": true,
                    "description": "",
                    "expr": { "kind": "path", "path": ["alpha_five"] }
                }
            ]
        }]
    }))
}

fn many_retrieved_columns_manifest_yaml() -> String {
    let columns = (0..60)
        .map(|index| {
            json!({
                "name": format!("alpha_{index:02}"),
                "type": "Utf8",
                "nullable": true,
                "description": "",
                "expr": { "kind": "path", "path": [format!("alpha_{index:02}")] }
            })
        })
        .collect::<Vec<_>>();

    manifest_yaml(&json!({
        "name": "many_retrieved_columns",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": "https://example.test",
        "tables": [{
            "name": "records",
            "description": "Alpha archive",
            "request": { "method": "GET", "path": "/records" },
            "response": {},
            "pagination": { "mode": "none" },
            "columns": columns
        }]
    }))
}

fn many_table_surfaces_manifest_yaml(table_count: usize) -> String {
    let tables = (0..table_count)
        .map(|index| {
            json!({
                "name": format!("table_{index:04}"),
                "description": "Synthetic table",
                "request": { "method": "GET", "path": "/records" },
                "response": {},
                "pagination": { "mode": "none" },
                "columns": [{
                    "name": "id",
                    "type": "Utf8",
                    "nullable": false,
                    "description": "Record id",
                    "expr": { "kind": "path", "path": ["id"] }
                }]
            })
        })
        .collect::<Vec<_>>();

    manifest_yaml(&json!({
        "name": "many_table_surfaces",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": "https://example.test",
        "tables": tables
    }))
}
