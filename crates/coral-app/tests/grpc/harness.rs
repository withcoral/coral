use std::fs;
use std::path::{Path, PathBuf};

use coral_api::v1::{
    CreateBundledSourceRequest, DeleteSourceRequest, DescribeTableRequest, DescribeTableResponse,
    DiscoverSourcesRequest, ExecuteSqlRequest, ExplainSqlRequest, GetSourceInfoRequest,
    GetSourceRequest, ImportSourceRequest, ListCatalogRequest, ListCatalogResponse,
    ListColumnsRequest, ListColumnsResponse, ListSourcesRequest, PaginationRequest,
    PaginationResponse, QueryPlan, QueryTestFailure, QueryTestResult, SearchCatalogRequest,
    SearchCatalogResponse, Source, SourceInfo, SourceSecret, SourceVariable, TableSummary,
    ValidateSourceRequest, ValidateSourceResponse, catalog_item, import_source_response,
    query_test_result,
};
use coral_client::{
    AppClient, CatalogClient, QueryClient, SourceClient, batches_to_json_rows,
    decode_execute_sql_response, default_workspace,
    local::{RunningServer, ServerBuilder},
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tonic::{Request, Response};
use wiremock::{Mock, MockServer, Request as WiremockRequest, matchers::any};

pub(crate) struct GrpcHarness {
    temp_dir: TempDir,
    config_dir: PathBuf,
    app: AppClient,
    _server: RunningServer,
}

pub(crate) struct FailingHttpFixture {
    server: MockServer,
}

fn failing_http_error(_request: &WiremockRequest) -> std::io::Error {
    std::io::Error::other("fixture connection failure")
}

fn expect_unary<T>(result: Result<Response<T>, tonic::Status>, expectation: &str) -> T {
    result.expect(expectation).into_inner()
}

fn expect_unary_error<T>(
    result: Result<Response<T>, tonic::Status>,
    expectation: &str,
) -> tonic::Status {
    match result {
        Ok(_) => panic!("{expectation}"),
        Err(status) => status,
    }
}

impl GrpcHarness {
    pub(crate) async fn new() -> Self {
        let temp_dir = TempDir::new().expect("temp dir");
        let config_dir = temp_dir.path().join("coral-config");
        Self::start_with_parts(temp_dir, config_dir).await
    }

    pub(crate) async fn start_with_config_dir(config_dir: PathBuf) -> Self {
        let temp_dir = TempDir::new().expect("temp dir");
        Self::start_with_parts(temp_dir, config_dir).await
    }

    pub(crate) async fn start_with_config(raw_config: &str) -> Self {
        let temp_dir = TempDir::new().expect("temp dir");
        let config_dir = temp_dir.path().join("coral-config");
        fs::create_dir_all(&config_dir).expect("create config dir");
        fs::write(config_dir.join("config.toml"), raw_config).expect("write config");
        Self::start_with_parts(temp_dir, config_dir).await
    }

    async fn start_with_parts(temp_dir: TempDir, config_dir: PathBuf) -> Self {
        ensure_file_credentials_config(&config_dir);
        let server = ServerBuilder::new()
            .with_config_dir(&config_dir)
            .start()
            .await
            .expect("start server");
        let app = AppClient::connect(server.endpoint_uri())
            .await
            .expect("connect client");
        Self {
            temp_dir,
            config_dir,
            app,
            _server: server,
        }
    }

    pub(crate) fn temp_path(&self) -> &Path {
        self.temp_dir.path()
    }

    pub(crate) fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    pub(crate) fn config_raw(&self) -> String {
        fs::read_to_string(self.config_dir.join("config.toml")).expect("read config")
    }

    pub(crate) fn source_client(&self) -> SourceClient {
        self.app.source_client()
    }

    pub(crate) fn catalog_client(&self) -> CatalogClient {
        self.app.catalog_client()
    }

    pub(crate) fn query_client(&self) -> QueryClient {
        self.app.query_client()
    }

    pub(crate) async fn import_source(
        &self,
        manifest_yaml: String,
        variables: Vec<SourceVariable>,
        secrets: Vec<SourceSecret>,
    ) -> Source {
        import_source_with_client(self.source_client(), manifest_yaml, variables, secrets).await
    }

    pub(crate) async fn import_source_without_inputs(&self, manifest_yaml: String) -> Source {
        self.import_source(manifest_yaml, Vec::new(), Vec::new())
            .await
    }

    pub(crate) async fn import_local_messages_source(&self) -> Source {
        self.import_source_without_inputs(fixture_manifest_yaml(self.temp_path()))
            .await
    }

    pub(crate) async fn import_multiple_table_messages_source(&self) -> Source {
        self.import_source_without_inputs(fixture_manifest_with_multiple_tables_yaml(
            self.temp_path(),
        ))
        .await
    }

    pub(crate) async fn import_searchy_source(&self) -> Source {
        self.import_source_without_inputs(fixture_manifest_with_functions_yaml())
            .await
    }

    pub(crate) async fn import_filtered_messages_source(&self) -> Source {
        self.import_source_without_inputs(fixture_manifest_with_required_filter_yaml())
            .await
    }

    pub(crate) async fn import_secured_messages_source(&self) -> Source {
        self.import_source(
            fixture_manifest_with_inputs_yaml(),
            vec![source_variable("API_BASE", "https://example.com")],
            vec![source_secret("API_TOKEN", "secret-token")],
        )
        .await
    }

    pub(crate) async fn import_source_error(
        &self,
        manifest_yaml: String,
        variables: Vec<SourceVariable>,
        secrets: Vec<SourceSecret>,
    ) -> tonic::Status {
        expect_unary_error(
            self.source_client()
                .import_source(Request::new(import_source_request(
                    manifest_yaml,
                    variables,
                    secrets,
                )))
                .await,
            "import source should fail",
        )
    }

    pub(crate) async fn create_bundled_source(
        &self,
        name: &str,
        variables: Vec<SourceVariable>,
        secrets: Vec<SourceSecret>,
    ) -> Source {
        expect_unary(
            self.source_client()
                .create_bundled_source(Request::new(create_bundled_source_request(
                    name, variables, secrets,
                )))
                .await,
            "create bundled source",
        )
        .source
        .expect("create bundled source response")
    }

    pub(crate) async fn create_bundled_source_error(
        &self,
        name: &str,
        variables: Vec<SourceVariable>,
        secrets: Vec<SourceSecret>,
    ) -> tonic::Status {
        expect_unary_error(
            self.source_client()
                .create_bundled_source(Request::new(create_bundled_source_request(
                    name, variables, secrets,
                )))
                .await,
            "create bundled source should fail",
        )
    }

    pub(crate) async fn list_sources(&self) -> Vec<Source> {
        expect_unary(
            self.source_client()
                .list_sources(Request::new(ListSourcesRequest {
                    workspace: Some(default_workspace()),
                }))
                .await,
            "list sources",
        )
        .sources
    }

    pub(crate) async fn discover_sources(&self) -> Vec<SourceInfo> {
        expect_unary(
            self.source_client()
                .discover_sources(Request::new(DiscoverSourcesRequest {
                    workspace: Some(default_workspace()),
                }))
                .await,
            "discover sources",
        )
        .sources
    }

    pub(crate) async fn get_source(&self, name: &str) -> Source {
        expect_unary(
            self.source_client()
                .get_source(Request::new(GetSourceRequest {
                    workspace: Some(default_workspace()),
                    name: name.to_string(),
                }))
                .await,
            "get source",
        )
        .source
        .expect("get source response")
    }

    pub(crate) async fn get_source_error(&self, name: &str) -> tonic::Status {
        expect_unary_error(
            self.source_client()
                .get_source(Request::new(GetSourceRequest {
                    workspace: Some(default_workspace()),
                    name: name.to_string(),
                }))
                .await,
            "get source should fail",
        )
    }

    pub(crate) async fn get_source_info(&self, name: &str) -> coral_api::v1::SourceInfo {
        expect_unary(
            self.source_client()
                .get_source_info(Request::new(GetSourceInfoRequest {
                    workspace: Some(default_workspace()),
                    name: name.to_string(),
                }))
                .await,
            "get source info",
        )
        .source_info
        .expect("get source info response")
    }

    pub(crate) async fn delete_source(&self, name: &str) {
        expect_unary(
            self.source_client()
                .delete_source(Request::new(DeleteSourceRequest {
                    workspace: Some(default_workspace()),
                    name: name.to_string(),
                }))
                .await,
            "delete source",
        );
    }

    pub(crate) async fn delete_source_error(&self, name: &str) -> tonic::Status {
        expect_unary_error(
            self.source_client()
                .delete_source(Request::new(DeleteSourceRequest {
                    workspace: Some(default_workspace()),
                    name: name.to_string(),
                }))
                .await,
            "delete source should fail",
        )
    }

    pub(crate) async fn list_catalog(
        &self,
        schema_name: &str,
        kind: i32,
        pagination: Option<PaginationRequest>,
    ) -> ListCatalogResponse {
        expect_unary(
            self.catalog_client()
                .list_catalog(Request::new(ListCatalogRequest {
                    workspace: Some(default_workspace()),
                    schema_name: schema_name.to_string(),
                    kind,
                    pagination,
                }))
                .await,
            "list catalog",
        )
    }

    pub(crate) async fn search_catalog(
        &self,
        pattern: &str,
        ignore_case: bool,
        schema_name: &str,
        kind: i32,
        pagination: Option<PaginationRequest>,
    ) -> Result<SearchCatalogResponse, tonic::Status> {
        self.catalog_client()
            .search_catalog(Request::new(SearchCatalogRequest {
                workspace: Some(default_workspace()),
                pattern: pattern.to_string(),
                ignore_case,
                schema_name: schema_name.to_string(),
                kind,
                pagination,
            }))
            .await
            .map(tonic::Response::into_inner)
    }

    pub(crate) async fn list_columns(
        &self,
        schema_name: &str,
        table_name: &str,
        pattern: Option<&str>,
        required_only: bool,
    ) -> Result<ListColumnsResponse, tonic::Status> {
        self.catalog_client()
            .list_columns(Request::new(ListColumnsRequest {
                workspace: Some(default_workspace()),
                schema_name: schema_name.to_string(),
                table_name: table_name.to_string(),
                pattern: pattern.map(str::to_string),
                ignore_case: true,
                required_only,
                pagination: None,
            }))
            .await
            .map(tonic::Response::into_inner)
    }

    pub(crate) async fn describe_table(
        &self,
        schema_name: &str,
        table_name: impl Into<String>,
    ) -> DescribeTableResponse {
        expect_unary(
            self.catalog_client()
                .describe_table(Request::new(DescribeTableRequest {
                    workspace: Some(default_workspace()),
                    schema_name: schema_name.to_string(),
                    table_name: table_name.into(),
                }))
                .await,
            "describe table",
        )
    }

    pub(crate) async fn list_tables(&self) -> Vec<TableSummary> {
        self.list_catalog("", 1, Some(page(0, 0)))
            .await
            .items
            .into_iter()
            .filter_map(|item| match item.item {
                Some(catalog_item::Item::Table(table)) => Some(table),
                Some(catalog_item::Item::TableFunction(_)) | None => None,
            })
            .collect()
    }

    pub(crate) async fn validate_source(&self, source_name: &str) -> ValidateSourceResponse {
        expect_unary(
            self.source_client()
                .validate_source(Request::new(ValidateSourceRequest {
                    workspace: Some(default_workspace()),
                    name: source_name.to_string(),
                }))
                .await,
            "validate source",
        )
    }

    pub(crate) async fn validate_source_error(&self, source_name: &str) -> tonic::Status {
        expect_unary_error(
            self.source_client()
                .validate_source(Request::new(ValidateSourceRequest {
                    workspace: Some(default_workspace()),
                    name: source_name.to_string(),
                }))
                .await,
            "validate source should fail",
        )
    }

    pub(crate) async fn explain_sql(&self, sql: &str) -> QueryPlan {
        expect_unary(
            self.query_client()
                .explain_sql(Request::new(ExplainSqlRequest {
                    workspace: Some(default_workspace()),
                    sql: sql.to_string(),
                }))
                .await,
            "explain sql",
        )
        .plan
        .expect("query plan")
    }

    pub(crate) async fn execute_sql_rows(&self, sql: &str) -> Vec<Value> {
        execute_sql_rows_with_client(self.query_client(), sql.to_string()).await
    }

    pub(crate) async fn execute_sql_error(&self, sql: impl Into<String>) -> tonic::Status {
        expect_unary_error(
            self.query_client()
                .execute_sql(Request::new(ExecuteSqlRequest {
                    workspace: Some(default_workspace()),
                    sql: sql.into(),
                }))
                .await,
            "execute sql should fail",
        )
    }
}

pub(crate) fn source_variable(key: &str, value: &str) -> SourceVariable {
    SourceVariable {
        key: key.to_string(),
        value: value.to_string(),
    }
}

pub(crate) fn source_variables(pairs: &[(&str, &str)]) -> Vec<SourceVariable> {
    pairs
        .iter()
        .map(|(key, value)| source_variable(key, value))
        .collect()
}

pub(crate) fn source_secret(key: &str, value: &str) -> SourceSecret {
    SourceSecret {
        key: key.to_string(),
        value: value.to_string(),
    }
}

pub(crate) fn source_secrets(pairs: &[(&str, &str)]) -> Vec<SourceSecret> {
    pairs
        .iter()
        .map(|(key, value)| source_secret(key, value))
        .collect()
}

pub(crate) fn page(limit: u32, offset: u32) -> PaginationRequest {
    PaginationRequest { limit, offset }
}

pub(crate) fn assert_pagination(
    pagination: Option<PaginationResponse>,
    total_count: u32,
    limit: u32,
    offset: u32,
    has_more: bool,
) {
    let pagination = pagination.expect("pagination");
    assert_eq!(pagination.total_count, total_count);
    assert_eq!(pagination.limit, limit);
    assert_eq!(pagination.offset, offset);
    assert_eq!(pagination.has_more, has_more);
    assert_eq!(
        pagination.next_offset,
        if has_more { offset + limit } else { 0 }
    );
}

pub(crate) fn assert_table_present(tables: &[TableSummary], schema_name: &str) {
    assert!(tables.iter().any(|table| table.schema_name == schema_name));
}

pub(crate) fn assert_table_absent(tables: &[TableSummary], schema_name: &str) {
    assert!(!tables.iter().any(|table| table.schema_name == schema_name));
}

pub(crate) fn assert_query_test_failure(result: &QueryTestResult, expected_message: Option<&str>) {
    assert!(matches!(
        &result.outcome,
        Some(query_test_result::Outcome::Failure(QueryTestFailure { error_message }))
            if expected_message == Some(error_message.as_str())
                || expected_message.is_none() && !error_message.is_empty()
    ));
}

pub(crate) async fn import_source_with_client(
    mut client: SourceClient,
    manifest_yaml: String,
    variables: Vec<SourceVariable>,
    secrets: Vec<SourceSecret>,
) -> Source {
    let mut stream = client
        .import_source(Request::new(import_source_request(
            manifest_yaml,
            variables,
            secrets,
        )))
        .await
        .expect("import source")
        .into_inner();
    stream
        .message()
        .await
        .expect("import source stream")
        .and_then(|response| match response.event {
            Some(import_source_response::Event::Source(source)) => Some(source),
            _ => None,
        })
        .expect("import source response")
}

pub(crate) async fn execute_sql_rows_with_client(
    mut client: QueryClient,
    sql: String,
) -> Vec<Value> {
    let response = expect_unary(
        client
            .execute_sql(Request::new(ExecuteSqlRequest {
                workspace: Some(default_workspace()),
                sql,
            }))
            .await,
        "execute sql",
    );
    batches_to_json_rows(
        decode_execute_sql_response(&response)
            .expect("decode query response")
            .batches(),
    )
    .expect("query rows")
}

pub(crate) fn import_source_request(
    manifest_yaml: String,
    variables: Vec<SourceVariable>,
    secrets: Vec<SourceSecret>,
) -> ImportSourceRequest {
    ImportSourceRequest {
        workspace: Some(default_workspace()),
        manifest_yaml,
        variables,
        secrets,
        oauth_credential_retrievals: Vec::new(),
    }
}

fn create_bundled_source_request(
    name: &str,
    variables: Vec<SourceVariable>,
    secrets: Vec<SourceSecret>,
) -> CreateBundledSourceRequest {
    CreateBundledSourceRequest {
        workspace: Some(default_workspace()),
        name: name.to_string(),
        variables,
        secrets,
    }
}

pub(crate) fn assert_status_contains(error: &tonic::Status, code: tonic::Code, expected: &str) {
    assert_eq!(error.code(), code);
    assert!(
        error.message().contains(expected),
        "expected error to contain {expected:?}, got: {}",
        error.message()
    );
}

pub(crate) fn write_source_secrets(config_dir: &Path, source_name: &str, raw: &str) {
    let secret_dir = source_dir(config_dir, source_name);
    fs::create_dir_all(&secret_dir).expect("create secret dir");
    fs::write(secret_dir.join("secrets.env"), raw).expect("write secrets");
}

fn ensure_file_credentials_config(config_dir: &Path) {
    std::fs::create_dir_all(config_dir).expect("create config dir");
    let config_file = config_dir.join("config.toml");
    let raw = std::fs::read_to_string(&config_file).unwrap_or_default();
    if raw.contains("[credentials]") {
        return;
    }
    let separator = if raw.is_empty() || raw.ends_with('\n') {
        ""
    } else {
        "\n"
    };
    let updated = format!("{raw}{separator}\n[credentials]\nstorage = \"file\"\n");
    std::fs::write(config_file, updated).expect("write test credential config");
}

impl FailingHttpFixture {
    pub(crate) async fn new() -> Self {
        let server = MockServer::start().await;
        Mock::given(any())
            .respond_with_err(failing_http_error)
            .mount(&server)
            .await;
        Self { server }
    }

    pub(crate) fn manifest_yaml(&self) -> String {
        self.manifest_yaml_with_test_queries(&[])
    }

    pub(crate) fn manifest_yaml_with_test_queries(&self, test_queries: &[&str]) -> String {
        source_manifest_yaml(
            "unreachable_messages",
            "http",
            json!({
            "base_url": self.server.uri(),
            "test_queries": test_queries,
            "tables": [http_messages_table("Unreachable messages")],
            }),
        )
    }
}

pub(crate) fn fixture_manifest_yaml(root: &Path) -> String {
    fixture_manifest_with_test_queries_yaml(root, &[])
}

fn fixture_messages_source(root: &Path) -> Value {
    let data_dir = root.join("fixture-data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    fs::write(
        data_dir.join("messages.jsonl"),
        r#"{"type":"user","sessionId":"s1","text":"hello"}
{"type":"assistant","sessionId":"s1","text":"world"}
"#,
    )
    .expect("write jsonl");
    json!({
        "location": format!("file://{}/", data_dir.display()),
        "glob": "**/*.jsonl",
    })
}

fn fixture_messages_table(name: &str, description: &str, source: &Value) -> Value {
    json!({
        "name": name,
        "description": description,
        "format": "jsonl",
        "source": source,
        "columns": [
            {"name": "type", "type": "Utf8"},
            {"name": "sessionId", "type": "Utf8"},
            {"name": "text", "type": "Utf8"},
        ],
    })
}

fn http_messages_table(description: &str) -> Value {
    json!({
        "name": "messages",
        "description": description,
        "request": {
            "method": "GET",
            "path": "/messages",
        },
        "response": {},
        "columns": [
            {"name": "id", "type": "Utf8"},
        ],
    })
}

pub(crate) fn fixture_manifest_with_multiple_tables_yaml(root: &Path) -> String {
    let table_source = fixture_messages_source(root);
    let tables = [
        ("events", "Fixture events"),
        ("messages", "Fixture messages"),
        ("sessions", "Fixture sessions"),
    ]
    .into_iter()
    .map(|(name, description)| fixture_messages_table(name, description, &table_source))
    .collect::<Vec<_>>();

    source_manifest_yaml("local_messages", "file", json!({ "tables": tables }))
}

pub(crate) fn fixture_manifest_with_required_filter_yaml() -> String {
    source_manifest_yaml(
        "filtered_messages",
        "http",
        json!({
        "base_url": "https://example.com",
        "tables": [{
            "name": "messages",
            "description": "Filtered messages",
            "request": {
                "method": "GET",
                "path": "/messages",
                "query": [
                    { "name": "channel", "from": "filter", "key": "channel" }
                ],
            },
            "response": {},
            "columns": [
                {"name": "channel", "type": "Utf8"},
                {"name": "text", "type": "Utf8"},
            ],
            "filters": [
                { "name": "channel", "required": true }
            ],
        }],
        }),
    )
}

pub(crate) fn fixture_manifest_with_functions_yaml() -> String {
    source_manifest_yaml(
        "searchy",
        "http",
        json!({
        "base_url": "https://example.com",
        "tables": [{
            "name": "placeholder",
            "description": "Placeholder table",
            "request": {
                "method": "GET",
                "path": "/placeholder",
            },
            "columns": [
                { "name": "id", "type": "Utf8" },
            ],
        }],
        "functions": [
            {
                "name": "lookup_issue",
                "description": "Lookup issue",
                "args": [
                    {
                        "name": "number",
                        "required": true,
                        "bind": { "arg": "number" },
                    },
                ],
                "request": {
                    "method": "GET",
                    "path": "/issues/{{arg.number}}",
                },
                "response": {},
                "columns": [
                    { "name": "title", "type": "Utf8", "description": "Issue title" },
                ],
            },
            search_issues_function(true),
        ],
        }),
    )
}

pub(crate) fn fixture_function_only_manifest_yaml() -> String {
    source_manifest_yaml(
        "searchy",
        "http",
        json!({
            "base_url": "https://example.com",
            "functions": [search_issues_function(false)],
        }),
    )
}

fn search_issues_function(include_search_mode: bool) -> Value {
    let mut args = vec![json!({
        "name": "q",
        "required": true,
        "bind": { "arg": "q" },
    })];
    let mut query = vec![json!({ "name": "q", "from": "arg", "key": "q" })];
    let mut columns = vec![json!({
        "name": "title",
        "type": "Utf8",
        "description": "Issue title",
    })];
    if include_search_mode {
        args.push(json!({
            "name": "mode",
            "values": ["lexical", "semantic", "hybrid"],
            "bind": { "arg": "search_type" },
        }));
        query.push(json!({ "name": "search_type", "from": "arg", "key": "search_type" }));
        columns.push(json!({ "name": "score", "type": "Float64" }));
    }

    json!({
        "name": "search_issues",
        "description": "Search issues",
        "args": args,
        "request": {
            "method": "GET",
            "path": "/search/issues",
            "query": query,
        },
        "response": {
            "rows_path": ["items"],
        },
        "columns": columns,
    })
}

pub(crate) fn fixture_manifest_with_test_queries_yaml(
    root: &Path,
    test_queries: &[&str],
) -> String {
    let table_source = fixture_messages_source(root);
    source_manifest_yaml(
        "local_messages",
        "file",
        json!({
            "test_queries": test_queries,
            "tables": [fixture_messages_table("messages", "Fixture messages", &table_source)],
        }),
    )
}

pub(crate) fn fixture_manifest_with_inputs_yaml() -> String {
    secured_messages_manifest_yaml(
        "secured_messages",
        &json!({ "kind": "variable", "default": "https://example.com" }),
        "Secured messages",
    )
}

pub(crate) fn fixture_manifest_with_required_inputs_yaml() -> String {
    secured_messages_manifest_yaml(
        "required_messages",
        &json!({ "kind": "variable" }),
        "Required-input messages",
    )
}

fn secured_messages_manifest_yaml(name: &str, api_base: &Value, description: &str) -> String {
    source_manifest_yaml(
        name,
        "http",
        json!({
        "inputs": {
            "API_BASE": api_base,
            "API_TOKEN": { "kind": "secret" },
        },
        "base_url": "{{input.API_BASE}}",
        "auth": {
            "type": "HeaderAuth",
            "headers": [{
                "name": "Authorization",
                "from": "template",
                "template": "Bearer {{input.API_TOKEN}}",
            }],
        },
        "tables": [http_messages_table(description)],
        }),
    )
}

pub(crate) fn invalid_manifest_yaml() -> String {
    source_manifest_yaml_with_version(
        "demo",
        "1.0.0",
        "http",
        json!({
        "schema": "demo",
        "tables": [http_messages_table("Demo messages")],
        }),
    )
}

fn source_manifest_yaml(name: &str, backend: &str, body: Value) -> String {
    source_manifest_yaml_with_version(name, "0.1.0", backend, body)
}

fn source_manifest_yaml_with_version(
    name: &str,
    version: &str,
    backend: &str,
    body: Value,
) -> String {
    let Value::Object(mut manifest) = body else {
        panic!("test source manifest body must be an object");
    };
    for (key, value) in [
        ("name", json!(name)),
        ("version", json!(version)),
        ("dsl_version", json!(3)),
        ("backend", json!(backend)),
    ] {
        manifest.insert(key.to_string(), value);
    }
    manifest_yaml(&Value::Object(manifest))
}

fn manifest_yaml(value: &Value) -> String {
    serde_yaml::to_string(value).expect("serialize manifest yaml")
}

pub(crate) fn sources_root(config_dir: &Path) -> PathBuf {
    config_dir
        .join("workspaces")
        .join("default")
        .join("sources")
}

pub(crate) fn source_dir(config_dir: &Path, source_name: &str) -> PathBuf {
    sources_root(config_dir).join(source_name)
}
