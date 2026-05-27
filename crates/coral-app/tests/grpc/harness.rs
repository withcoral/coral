use std::fs;
use std::path::{Path, PathBuf};

use coral_api::v1::{
    ExecuteSqlRequest, ImportSourceRequest, ListCatalogRequest, ListSourcesRequest,
    PaginationRequest, Source, SourceSecret, SourceVariable, TableSummary, ValidateSourceRequest,
    ValidateSourceResponse, catalog_item, import_source_response,
};
use coral_client::{
    AppClient, CatalogClient, QueryClient, SourceClient, batches_to_json_rows,
    decode_execute_sql_response, default_workspace,
    local::{RunningServer, ServerBuilder},
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tonic::Request;

pub(crate) struct GrpcHarness {
    temp_dir: TempDir,
    config_dir: PathBuf,
    app: AppClient,
    _server: RunningServer,
}

pub(crate) struct FailingHttpFixture {
    base_url: String,
    task: tokio::task::JoinHandle<()>,
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
        let mut stream = self
            .source_client()
            .import_source(Request::new(ImportSourceRequest {
                workspace: Some(default_workspace()),
                manifest_yaml,
                variables,
                secrets,
                oauth_credential_retrievals: Vec::new(),
            }))
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

    pub(crate) async fn list_sources(&self) -> Vec<Source> {
        self.source_client()
            .list_sources(Request::new(ListSourcesRequest {
                workspace: Some(default_workspace()),
            }))
            .await
            .expect("list sources")
            .into_inner()
            .sources
    }

    pub(crate) async fn list_tables(&self) -> Vec<TableSummary> {
        self.catalog_client()
            .list_catalog(Request::new(ListCatalogRequest {
                workspace: Some(default_workspace()),
                schema_name: String::new(),
                kind: 1,
                pagination: Some(PaginationRequest {
                    limit: 0,
                    offset: 0,
                }),
            }))
            .await
            .expect("list catalog")
            .into_inner()
            .items
            .into_iter()
            .filter_map(|item| match item.item {
                Some(catalog_item::Item::Table(table)) => Some(table),
                Some(catalog_item::Item::TableFunction(_)) | None => None,
            })
            .collect()
    }

    pub(crate) async fn validate_source(&self, source_name: &str) -> ValidateSourceResponse {
        self.source_client()
            .validate_source(Request::new(ValidateSourceRequest {
                workspace: Some(default_workspace()),
                name: source_name.to_string(),
            }))
            .await
            .expect("validate source")
            .into_inner()
    }

    pub(crate) async fn execute_sql_rows(&self, sql: &str) -> Vec<Value> {
        let response = self
            .query_client()
            .execute_sql(Request::new(ExecuteSqlRequest {
                workspace: Some(default_workspace()),
                sql: sql.to_string(),
            }))
            .await
            .expect("execute sql")
            .into_inner();
        batches_to_json_rows(
            decode_execute_sql_response(&response)
                .expect("decode query response")
                .batches(),
        )
        .expect("query rows")
    }
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind failing http fixture");
        let addr = listener.local_addr().expect("fixture local addr");
        let task = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.expect("accept fixture connection");
                drop(socket);
            }
        });

        Self {
            base_url: format!("http://{addr}"),
            task,
        }
    }

    pub(crate) fn manifest_yaml(&self) -> String {
        self.manifest_yaml_with_test_queries(&[])
    }

    pub(crate) fn manifest_yaml_with_test_queries(&self, test_queries: &[&str]) -> String {
        manifest_yaml(&json!({
            "name": "unreachable_messages",
            "version": "0.1.0",
            "dsl_version": 3,
            "backend": "http",
            "base_url": self.base_url,
            "test_queries": test_queries,
            "tables": [{
                "name": "messages",
                "description": "Unreachable messages",
                "request": {
                    "method": "GET",
                    "path": "/messages",
                },
                "response": {},
                "columns": [
                    {"name": "id", "type": "Utf8"},
                ],
            }],
        }))
    }
}

impl Drop for FailingHttpFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(crate) fn fixture_manifest_yaml(root: &Path) -> String {
    fixture_manifest_with_test_queries_yaml(root, &[])
}

pub(crate) fn fixture_manifest_with_multiple_tables_yaml(root: &Path) -> String {
    let data_dir = root.join("fixture-data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    fs::write(
        data_dir.join("messages.jsonl"),
        r#"{"type":"user","sessionId":"s1","text":"hello"}
{"type":"assistant","sessionId":"s1","text":"world"}
"#,
    )
    .expect("write jsonl");
    let table_source = json!({
        "location": format!("file://{}/", data_dir.display()),
        "glob": "**/*.jsonl",
    });
    let table_columns = json!([
        {"name": "type", "type": "Utf8"},
        {"name": "sessionId", "type": "Utf8"},
        {"name": "text", "type": "Utf8"},
    ]);
    manifest_yaml(&json!({
        "name": "local_messages",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [
            {
                "name": "events",
                "description": "Fixture events",
                "format": "jsonl",
                "source": table_source.clone(),
                "columns": table_columns.clone(),
            },
            {
                "name": "messages",
                "description": "Fixture messages",
                "format": "jsonl",
                "source": table_source.clone(),
                "columns": table_columns.clone(),
            },
            {
                "name": "sessions",
                "description": "Fixture sessions",
                "format": "jsonl",
                "source": table_source,
                "columns": table_columns,
            },
        ],
    }))
}

pub(crate) fn fixture_manifest_with_required_filter_yaml() -> String {
    manifest_yaml(&json!({
        "name": "filtered_messages",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
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
    }))
}

pub(crate) fn fixture_manifest_with_functions_yaml() -> String {
    manifest_yaml(&json!({
        "name": "searchy",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
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
            {
                "name": "search_issues",
                "description": "Search issues",
                "args": [
                    {
                        "name": "q",
                        "required": true,
                        "bind": { "arg": "q" },
                    },
                    {
                        "name": "mode",
                        "values": ["lexical", "semantic", "hybrid"],
                        "bind": { "arg": "search_type" },
                    },
                ],
                "request": {
                    "method": "GET",
                    "path": "/search/issues",
                    "query": [
                        { "name": "q", "from": "arg", "key": "q" },
                        { "name": "search_type", "from": "arg", "key": "search_type" },
                    ],
                },
                "response": {
                    "rows_path": ["items"],
                },
                "columns": [
                    { "name": "title", "type": "Utf8", "description": "Issue title" },
                    { "name": "score", "type": "Float64" },
                ],
            },
        ],
    }))
}

pub(crate) fn fixture_function_only_manifest_yaml() -> String {
    manifest_yaml(&json!({
        "name": "searchy",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": "https://example.com",
        "functions": [{
            "name": "search_issues",
            "description": "Search issues",
            "args": [{
                "name": "q",
                "required": true,
                "bind": { "arg": "q" },
            }],
            "request": {
                "method": "GET",
                "path": "/search/issues",
                "query": [
                    { "name": "q", "from": "arg", "key": "q" },
                ],
            },
            "response": {
                "rows_path": ["items"],
            },
            "columns": [
                { "name": "title", "type": "Utf8", "description": "Issue title" },
            ],
        }],
    }))
}

pub(crate) fn fixture_manifest_with_test_queries_yaml(
    root: &Path,
    test_queries: &[&str],
) -> String {
    let data_dir = root.join("fixture-data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    fs::write(
        data_dir.join("messages.jsonl"),
        r#"{"type":"user","sessionId":"s1","text":"hello"}
{"type":"assistant","sessionId":"s1","text":"world"}
"#,
    )
    .expect("write jsonl");
    manifest_yaml(&json!({
        "name": "local_messages",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "test_queries": test_queries,
        "tables": [{
            "name": "messages",
            "description": "Fixture messages",
            "format": "jsonl",
            "source": {
                "location": format!("file://{}/", data_dir.display()),
                "glob": "**/*.jsonl",
            },
            "columns": [
                {"name": "type", "type": "Utf8"},
                {"name": "sessionId", "type": "Utf8"},
                {"name": "text", "type": "Utf8"},
            ],
        }],
    }))
}

pub(crate) fn fixture_manifest_with_inputs_yaml() -> String {
    manifest_yaml(&json!({
        "name": "secured_messages",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "inputs": {
            "API_BASE": { "kind": "variable", "default": "https://example.com" },
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
        "tables": [{
            "name": "messages",
            "description": "Secured messages",
            "request": {
                "method": "GET",
                "path": "/messages",
            },
            "response": {},
            "columns": [
                {"name": "id", "type": "Utf8"},
            ],
        }],
    }))
}

pub(crate) fn fixture_manifest_with_required_inputs_yaml() -> String {
    manifest_yaml(&json!({
        "name": "required_messages",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "inputs": {
            "API_BASE": { "kind": "variable" },
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
        "tables": [{
            "name": "messages",
            "description": "Required-input messages",
            "request": {
                "method": "GET",
                "path": "/messages",
            },
            "response": {},
            "columns": [
                {"name": "id", "type": "Utf8"},
            ],
        }],
    }))
}

pub(crate) fn invalid_manifest_yaml() -> String {
    manifest_yaml(&json!({
        "name": "demo",
        "schema": "demo",
        "version": "1.0.0",
        "dsl_version": 3,
        "backend": "http",
        "tables": [{
            "name": "messages",
            "description": "Demo messages",
            "request": {
                "method": "GET",
                "path": "/messages",
            },
            "response": {},
            "columns": [
                {"name": "id", "type": "Utf8"},
            ],
        }],
    }))
}

fn manifest_yaml(value: &Value) -> String {
    serde_yaml::to_string(value).expect("serialize manifest yaml")
}

pub(crate) fn source_dir(config_dir: &Path, source_name: &str) -> PathBuf {
    config_dir
        .join("workspaces")
        .join("default")
        .join("sources")
        .join(source_name)
}
